//! ok's ChangeLists from the command line (docs/changelists.md, "From the command
//! line"): the client of ok control's ChangeLists service, and how `jj cl status` shows a CL
//! and decides when following it ends. The jj binary's commands call it.

pub mod client;
pub mod render;

pub use client::Control;
pub use client::Event;
pub use client::FollowEvent;
pub use client::Followed;
pub use ok_proto::ok::control::v1 as pb;

#[cfg(test)]
mod tests;
