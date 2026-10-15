//! The device login (OAuth's device authorization grant, as ok identity runs it): a new key, a code
//! the person approves in a browser, and the certificate ok identity issues for the key once they
//! do.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ok_proto::ok::identity::v1 as pb;

use crate::{Auth, Authed, Credential, Error, Person, Server, load, new_key, public_key_der, save};

/// What the person is asked to approve.
#[derive(Clone, Debug)]
pub struct Approval {
    /// The code: XXXX-XXXX.
    pub user_code: String,
    /// Where to approve it.
    pub uri: String,
    /// The same, with the code in it: what to open, or show as a link.
    pub uri_complete: String,
}

/// Signs this machine in to `server`: makes a key, starts a device login, tells the person through
/// `show` where to approve it, and polls until they do. The credential is saved in `dir` and
/// returned.
pub async fn login(
    server: &Server,
    dir: &Path,
    label: &str,
    show: impl FnOnce(&Approval),
) -> Result<Credential, Error> {
    let key = new_key();
    let channel = server.connect().await?;
    let mut client = pb::device_login_client::DeviceLoginClient::new(channel.clone());
    let start = client
        .start(pb::StartRequest {
            public_key: public_key_der(&key),
            label: label.to_owned(),
        })
        .await
        .map_err(|s| Error::Login(s.message().to_owned()))?
        .into_inner();
    show(&Approval {
        user_code: start.user_code.clone(),
        uri: start.verification_uri.clone(),
        uri_complete: start.verification_uri_complete.clone(),
    });
    let mut interval = Duration::from_secs(start.interval.max(1) as u64);
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(start.expires_in.max(0) as u64);
    loop {
        tokio::time::sleep(interval).await;
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Login(
                "the code expired before it was approved: sign in again".into(),
            ));
        }
        let poll = client
            .poll(pb::PollRequest {
                device_code: start.device_code.clone(),
            })
            .await
            .map_err(|s| Error::Login(s.message().to_owned()))?
            .into_inner();
        match pb::PollState::try_from(poll.state).unwrap_or(pb::PollState::Unspecified) {
            pb::PollState::Pending | pb::PollState::Unspecified => {}
            pb::PollState::SlowDown => interval += Duration::from_secs(5),
            pb::PollState::Denied => return Err(Error::Login("the sign-in was denied".into())),
            pb::PollState::Expired => {
                return Err(Error::Login(
                    "the code expired before it was approved: sign in again".into(),
                ));
            }
            pb::PollState::Approved => {
                let der = poll
                    .certificate
                    .and_then(|c| c.chain.into_iter().next())
                    .ok_or_else(|| {
                        Error::Login(
                            "ok identity approved the login and sent no certificate".into(),
                        )
                    })?;
                let cred = Credential::from_parts(server.clone(), der.clone(), key.clone())?;
                let person = person(&channel, &cred).await;
                save(dir, server, &der, &key, person.as_ref())?;
                return load(dir);
            }
        }
    }
}

/// Who the credential is, as the directory says: what the person's commits are signed with.
async fn person(channel: &tonic::transport::Channel, cred: &Credential) -> Option<Person> {
    let svc = Authed::new(channel.clone(), Auth::Static(Arc::new(cred.clone())));
    let mut dir = pb::directory_client::DirectoryClient::new(svc);
    let p = dir
        .get_principal(pb::GetPrincipalRequest {
            key: Some(pb::get_principal_request::Key::Name(cred.principal.clone())),
        })
        .await
        .ok()?
        .into_inner()
        .principal?;
    Some(Person {
        name: p.name,
        email: p.email,
        display_name: p.display_name,
    })
}
