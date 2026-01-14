# DB/DC Seminar Scheduler

Streamlit-based scheduler with a Doodle-like flow for small groups (5–10). Supports admin-managed polls, guest voting, password gating, calendar exports, and optional Synology CSV export.

## Features
- Admin vs guest modes (sidebar toggle).
- Admin: create/edit/delete polls, optional poll password, interactively choose which generated slots to use, push finalized slot to Synology Calendar via REST.
- Guest: vote with button-style slot picks, participant password required to load/update your own votes, per-day timeline preview, iCal download + Google Calendar link for your first selected slot.
- i18n (ko/en switch), mobile-friendly wrapping timeline/buttons.
- SQLite persistence in `scheduler.db`, lightweight auto-migrations on start.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
 pip install streamlit pandas requests
 streamlit run app.py
```

## Configuration
- Admin password: `SCHEDULER_APP_PASSWORD` env or `.streamlit/secrets.toml` with `app_password = "..."` (default `changeme`).
- Poll password: set when creating/updating a poll; required for guests to vote.
- Participant password: each voter sets one to load/update their own answers.
- Synology Calendar API: provide base URL (e.g., `https://your-nas:5001`), Synology account/password, and Calendar ID in the admin export panel. Uses `SYNO.API.Auth` + `SYNO.Cal.Event` (version 3) as per Synology Calendar API Guide. Ensure your NAS allows HTTPS API access and the account has Calendar privileges.
- Finalization: Admins can confirm a slot (most voted or unanimous), which enables calendar exports (guest/admin) and produces an email draft. Synology upload uses the finalized slot if set; otherwise falls back to the top-voted slot.

## Usage
1. Admin mode (sidebar) → log in → create poll (date range, time window, slot length, optional poll password).
2. Share the poll ID. Guests pick the poll, enter poll password if set, enter their name + participant password, toggle slots with buttons, and save.
3. Guests can load their choices with the same name/password, and export their first selected slot to iCal/Google Calendar.
4. Admins can load polls into the form for edits, delete polls, and finalize a slot (unanimous or most-voted); finalization reveals an email draft and enables exports.
5. Admins can push the finalized (or top-voted if not finalized) slot to Synology Calendar via REST. Provide URL/account/password/calendar ID; the slot is sent as an event with the poll title/description.
6. Guests can export to iCal/Google Calendar only after a slot is finalized by the admin.

## Notes
- Mobile: buttons and timeline wrap on small screens.
- Data lives in `scheduler.db` in the project root; keep backups as needed.***
