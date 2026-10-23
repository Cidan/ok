// passkeys.js runs the browser half of ok's passkey ceremonies: signing in (#passkey-login),
// adding a passkey to a signed-in account (#passkey-add), and approving what ok secrets asked
// (#passkey-approve). Each asks the server to begin — the
// options for the authenticator and the ceremony's ID — runs the authenticator, and posts its
// answer to finish the ceremony.
(() => {
  const b64uToBuf = (s) => {
    const pad = s.length % 4 === 0 ? '' : '='.repeat(4 - (s.length % 4));
    const bin = atob(s.replace(/-/g, '+').replace(/_/g, '/') + pad);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    return buf.buffer;
  };
  const bufToB64u = (buf) => {
    const bytes = new Uint8Array(buf);
    let bin = '';
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  };

  const statusEl = () => document.getElementById('status');
  const setStatus = (kind, msg) => {
    const el = statusEl();
    if (!el) return;
    const skin =
      kind === 'error'
        ? 'bg-rose-500/15 text-rose-200 ring-1 ring-rose-500/30'
        : kind === 'ok'
          ? 'bg-emerald-500/15 text-emerald-200 ring-1 ring-emerald-500/30'
          : 'bg-slate-700/40 text-slate-200 ring-1 ring-white/10';
    el.className = 'mt-6 rounded-lg px-4 py-3 text-sm ' + skin;
    el.textContent = msg;
  };
  const errText = (err) => (err?.message ? err.message : String(err));

  // post sends JSON and returns the parsed answer, or throws the server's message.
  const post = async (url, body) => {
    const resp = await fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: body === undefined ? '{}' : JSON.stringify(body),
    });
    const text = await resp.text();
    if (!resp.ok) throw new Error(text || resp.statusText);
    return text ? JSON.parse(text) : {};
  };

  const encodeCredential = (cred) => {
    const out = { id: cred.id, rawId: bufToB64u(cred.rawId), type: cred.type, response: {} };
    const r = cred.response;
    for (const k of [
      'clientDataJSON',
      'attestationObject',
      'authenticatorData',
      'signature',
      'userHandle',
    ]) {
      if (r[k]) out.response[k] = bufToB64u(r[k]);
    }
    if (typeof r.getTransports === 'function') out.response.transports = r.getTransports();
    out.clientExtensionResults = cred.getClientExtensionResults
      ? cred.getClientExtensionResults()
      : {};
    if (cred.authenticatorAttachment) out.authenticatorAttachment = cred.authenticatorAttachment;
    return out;
  };

  const login = document.getElementById('passkey-login');
  if (login) {
    login.addEventListener('click', async () => {
      login.disabled = true;
      setStatus('info', 'Waiting for your passkey…');
      try {
        const begun = await post(login.dataset.begin);
        const pk = begun.options.publicKey;
        pk.challenge = b64uToBuf(pk.challenge);
        if (Array.isArray(pk.allowCredentials)) {
          pk.allowCredentials = pk.allowCredentials.map((c) => ({ ...c, id: b64uToBuf(c.id) }));
        }
        const cred = await navigator.credentials.get({ publicKey: pk });
        const done = await post(
          login.dataset.finish +
            '?ceremony=' +
            encodeURIComponent(begun.ceremony) +
            '&next=' +
            encodeURIComponent(login.dataset.next || ''),
          encodeCredential(cred),
        );
        window.location.assign(done.next);
      } catch (err) {
        setStatus('error', errText(err));
        login.disabled = false;
      }
    });
  }

  const approve = document.getElementById('passkey-approve');
  if (approve) {
    approve.addEventListener('click', async () => {
      approve.disabled = true;
      setStatus('info', 'Waiting for your passkey…');
      try {
        const begun = await post(approve.dataset.begin);
        const pk = begun.options.publicKey;
        pk.challenge = b64uToBuf(pk.challenge);
        if (Array.isArray(pk.allowCredentials)) {
          pk.allowCredentials = pk.allowCredentials.map((c) => ({ ...c, id: b64uToBuf(c.id) }));
        }
        const cred = await navigator.credentials.get({ publicKey: pk });
        await post(
          approve.dataset.finish + '?ceremony=' + encodeURIComponent(begun.ceremony),
          encodeCredential(cred),
        );
        setStatus('ok', 'Approved. You can close this page.');
      } catch (err) {
        setStatus('error', errText(err));
        approve.disabled = false;
      }
    });
  }

  const add = document.getElementById('passkey-add');
  if (add) {
    add.addEventListener('click', async () => {
      const labelEl = document.getElementById('passkey-label');
      const label = labelEl?.value.trim() || 'Passkey';
      add.disabled = true;
      setStatus('info', 'Follow your authenticator…');
      try {
        const begun = await post(add.dataset.begin);
        const pk = begun.options.publicKey;
        pk.challenge = b64uToBuf(pk.challenge);
        pk.user.id = b64uToBuf(pk.user.id);
        if (Array.isArray(pk.excludeCredentials)) {
          pk.excludeCredentials = pk.excludeCredentials.map((c) => ({ ...c, id: b64uToBuf(c.id) }));
        }
        const cred = await navigator.credentials.create({ publicKey: pk });
        await post(
          add.dataset.finish +
            '?ceremony=' +
            encodeURIComponent(begun.ceremony) +
            '&label=' +
            encodeURIComponent(label),
          encodeCredential(cred),
        );
        setStatus('ok', 'Your passkey is added.');
        window.location.reload();
      } catch (err) {
        setStatus('error', errText(err));
        add.disabled = false;
      }
    });
  }
})();
