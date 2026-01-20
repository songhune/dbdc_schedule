# DB/DC Seminar Scheduler

Streamlit-based scheduler for small groups (5–10). Supports admin-managed polls, guest voting, password gating, and CSV export.

## Features
- Admin vs guest modes (sidebar toggle).
- Admin: create/edit/delete polls, interactively choose which generated slots to use, edit individual participant availability, CSV download, finalize to show an email draft.
- Guest: vote with button-style slot picks, participant password required to load/update your own votes (stored as hash), per-day timeline preview.
- i18n (ko/en switch), mobile-friendly wrapping timeline/buttons.
- SQLite persistence in `scheduler.db`, lightweight auto-migrations on start.

## Setup
1) Create a virtualenv and install deps:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2) Run the app:
```bash
streamlit run app.py
```
3) Open the URL Streamlit prints (usually http://localhost:8501).

## Configuration
- Admin password: `SCHEDULER_APP_PASSWORD` env or `.streamlit/secrets.toml` with `app_password = "..."` (default `changeme`).
- Poll password: optional; used to guard delete/finalize in admin actions (guests can vote without it).
- Participant password: each voter sets one to load/update their own answers (stored as hash).
- Finalization: Admins can confirm a slot (most voted or unanimous), which produces an email draft.

## Usage
1. Admin mode (sidebar):
   - Log in with the admin password.
   - Create a poll (date range/time window/slot length or interactively deselect slots; optional poll password).
   - Load a poll for edits or delete it. Finalize a slot (unanimous or most-voted) to show an email draft.
   - Edit a participant's availability if a correction is needed.
   - Download votes as CSV from the export panel.
2. Guest mode:
   - Pick the poll; simple view is default (uncheck to see timeline/filter details).
   - Enter name + participant password, toggle slots, and save. Use “Load my choices” with the same name/password to edit.

## Notes
- Mobile: buttons and timeline wrap on small screens.
- Data lives in `scheduler.db` in the project root; keep backups as needed.***
