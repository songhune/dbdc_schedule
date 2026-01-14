import os
import html
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from scheduler_core import (
    build_ics,
    generate_slots,
    get_conn,
    load_polls,
    slot_label,
)
from synology_client import push_synology_event, SynologyError

st.set_page_config(page_title="DB/DC Seminar Scheduler", layout="wide")

DEFAULT_ADMIN_PASS = "changeme"

TRANSLATIONS = {
    "ko": {
        "app_title": "DB/DC 세미나 스케줄러",
        "sidebar_info": "앱 전체 비밀번호는 환경변수 `SCHEDULER_APP_PASSWORD` 또는 `.streamlit/secrets.toml`의 `app_password`로 설정하세요. (기본값: changeme)",
        "login_title": "관리 비밀번호",
        "login_button": "로그인",
        "login_error": "비밀번호가 올바르지 않습니다.",
        "login_success": "로그인 완료",
        "create_title": "새 폴 생성",
        "poll_id": "폴 ID (짧은 코드)",
        "poll_id_help": "예: team-lunch-jan",
        "poll_title": "제목",
        "poll_desc": "설명",
        "poll_desc_default": "가능한 시간대를 선택해주세요.",
        "date_range": "날짜 범위",
        "date_help": "시작/종료 날짜를 선택하면 모든 날짜에 동일한 시간 슬롯을 만듭니다.",
        "start_time": "시작 시간",
        "end_time": "종료 시간",
        "slot_minutes": "슬롯 길이(분)",
        "create_submit": "폴 생성/덮어쓰기",
        "poll_id_required": "폴 ID는 필수입니다.",
        "date_required": "시작/종료 날짜를 모두 선택해주세요.",
        "time_invalid": "시작 시간은 종료 시간보다 빨라야 합니다.",
        "no_slots": "생성된 슬롯이 없습니다. 시간 범위와 길이를 확인하세요.",
        "poll_ready": "폴 `{poll_id}`이(가) 준비되었습니다. 오른쪽에서 공유/투표를 진행하세요.",
        "participation": "참여 및 현황",
        "no_polls": "아직 생성된 폴이 없습니다.",
        "open_polls": "열린 폴",
        "vote_section": "투표하기",
        "voter_name": "이름/닉네임",
        "comment": "메모 (선택)",
        "vote_submit": "저장",
        "name_required": "이름/닉네임을 입력해주세요.",
        "vote_saved": "응답이 저장되었습니다.",
        "responses": "응답",
        "popular": "가장 인기 있는 슬롯",
        "popular_col_slot": "슬롯",
        "popular_col_yes": "찬성",
        "no_responses": "아직 응답이 없습니다. 링크를 공유하고 투표를 받아보세요.",
        "export_title": "Synology NAS 내보내기 (공유폴더가 OS에서 마운트되어 있어야 합니다)",
        "export_path": "내보내기 경로",
        "export_button": "CSV로 내보내기",
        "export_need_path": "경로를 입력하세요.",
        "export_success": "내보내기 완료: {path}",
        "export_fail": "내보내기에 실패했습니다: {error}",
        "timeline_title": "타임라인 미리보기",
        "timeline_slots_badge": "총 {count} 슬롯",
        "language": "언어",
        "people_suffix": "명",
        "mode": "모드",
        "mode_admin": "관리자",
        "mode_guest": "참여자",
        "poll_password": "폴 비밀번호 (선택)",
        "poll_password_help": "투표시 입력해야 하는 비밀번호. 비우면 비번 없이 투표 가능.",
        "poll_password_prompt": "폴 비밀번호를 입력하세요",
        "access_needed": "투표하려면 폴 비밀번호가 필요합니다.",
        "delete_poll": "선택 폴 삭제",
        "delete_confirm": "삭제 확인용으로 폴 ID를 입력하세요",
        "delete_done": "폴이 삭제되었습니다.",
        "delete_fail": "폴 삭제에 실패했습니다: {error}",
        "edit_load": "선택 폴 불러와서 수정",
        "choose_slots": "가능한 슬롯을 버튼으로 선택하세요",
        "selected_count": "선택된 슬롯: {count}개",
        "load_my_vote": "내 선택 불러오기",
        "voter_pw": "참여자 비밀번호",
        "voter_pw_help": "내 응답을 수정/불러올 때 사용할 비밀번호",
        "voter_pw_need": "참여자 비밀번호를 입력해주세요.",
        "voter_pw_mismatch": "참여자 비밀번호가 일치하지 않습니다.",
        "add_calendar": "캘린더에 추가",
        "need_selection": "선택한 슬롯이 있어야 캘린더로 내보낼 수 있습니다.",
        "ical_download": "📅 iCal 다운로드",
        "google_add": "🗓 Google Calendar에 추가",
        "timeline_hint": "슬롯 위에 마우스를 올리면 선택자 목록이 보입니다.",
        "selected_slots": "선택한 슬롯 요약",
        "popular_names": "슬롯별 선택자",
        "syno_url": "Synology URL (예: https://your-nas:5001)",
        "syno_user": "Synology 계정",
        "syno_pass": "Synology 비밀번호",
        "syno_cal": "캘린더 ID",
        "syno_upload": "Synology 캘린더로 업로드",
        "syno_need_slot": "업로드할 슬롯을 결정할 수 없습니다. 투표를 먼저 받으세요.",
        "syno_success": "Synology 캘린더에 업로드되었습니다.",
        "syno_error": "Synology 업로드 실패: {error}",
    },
    "en": {
        "app_title": "DB/DC Seminar Scheduler",
        "sidebar_info": "Set the app password via env `SCHEDULER_APP_PASSWORD` or `.streamlit/secrets.toml` key `app_password`. (default: changeme)",
        "login_title": "Admin password",
        "login_button": "Log in",
        "login_error": "Incorrect password.",
        "login_success": "Logged in",
        "create_title": "Create poll",
        "poll_id": "Poll ID (short code)",
        "poll_id_help": "e.g., team-lunch-jan",
        "poll_title": "Title",
        "poll_desc": "Description",
        "poll_desc_default": "Select all time slots that work for you.",
        "date_range": "Date range",
        "date_help": "All dates share the same time slots.",
        "start_time": "Start time",
        "end_time": "End time",
        "slot_minutes": "Slot length (minutes)",
        "create_submit": "Create/overwrite poll",
        "poll_id_required": "Poll ID is required.",
        "date_required": "Select both start and end dates.",
        "time_invalid": "Start time must be before end time.",
        "no_slots": "No slots were generated. Check the time range and length.",
        "poll_ready": "Poll `{poll_id}` is ready. Share and vote on the right.",
        "participation": "Participation & status",
        "no_polls": "No polls yet.",
        "open_polls": "Open polls",
        "vote_section": "Vote",
        "voter_name": "Name/Nickname",
        "comment": "Note (optional)",
        "vote_submit": "Save",
        "name_required": "Please enter your name/nickname.",
        "vote_saved": "Response saved.",
        "responses": "Responses",
        "popular": "Top slots",
        "popular_col_slot": "Slot",
        "popular_col_yes": "Yes",
        "no_responses": "No responses yet. Share the link and collect votes.",
        "export_title": "Synology NAS export (shared folder must be mounted by OS)",
        "export_path": "Export path",
        "export_button": "Export CSV",
        "export_need_path": "Enter a path.",
        "export_success": "Exported to: {path}",
        "export_fail": "Export failed: {error}",
        "timeline_title": "Timeline preview",
        "timeline_slots_badge": "{count} slots",
        "language": "Language",
        "people_suffix": "people",
        "mode": "Mode",
        "mode_admin": "Admin",
        "mode_guest": "Guest",
        "poll_password": "Poll password (optional)",
        "poll_password_help": "Required for voting. Leave blank for open voting.",
        "poll_password_prompt": "Enter poll password",
        "access_needed": "Poll password required to vote.",
        "delete_poll": "Delete selected poll",
        "delete_confirm": "Type poll ID to confirm deletion",
        "delete_done": "Poll deleted.",
        "delete_fail": "Failed to delete poll: {error}",
        "edit_load": "Load selected poll for edit",
        "choose_slots": "Pick your slots (button toggle)",
        "selected_count": "Selected slots: {count}",
        "load_my_vote": "Load my choices",
        "voter_pw": "Participant password",
        "voter_pw_help": "Required to edit/load your responses",
        "voter_pw_need": "Enter your participant password.",
        "voter_pw_mismatch": "Participant password does not match.",
        "add_calendar": "Add to calendar",
        "need_selection": "Select at least one slot to export to calendar.",
        "ical_download": "📅 Download iCal",
        "google_add": "🗓 Add to Google Calendar",
        "timeline_hint": "Hover a slot to see who picked it.",
        "selected_slots": "Chosen slots by participant",
        "popular_names": "Slots with voters",
        "syno_url": "Synology URL (e.g., https://your-nas:5001)",
        "syno_user": "Synology account",
        "syno_pass": "Synology password",
        "syno_cal": "Calendar ID",
        "syno_upload": "Upload to Synology Calendar",
        "syno_need_slot": "Cannot determine a slot to upload. Collect votes first.",
        "syno_success": "Uploaded to Synology Calendar.",
        "syno_error": "Synology upload failed: {error}",
    },
}


def t(key: str, **kwargs) -> str:
    lang = st.session_state.get("lang", "ko")
    template = TRANSLATIONS.get(lang, {}).get(key, TRANSLATIONS["ko"].get(key, key))
    try:
        return template.format(**kwargs)
    except Exception:
        return template



def get_admin_password() -> str:
    try:
        secret = st.secrets["app_password"]
    except Exception:
        secret = None
    return secret or os.getenv("SCHEDULER_APP_PASSWORD", DEFAULT_ADMIN_PASS)


@st.cache_resource
def get_conn_cached():
    return get_conn()


def render_timeline(options_df: pd.DataFrame, summary_map: dict, voters_map: dict):
    """Draw a horizontal timeline per day with slot popularity intensity and voter tooltip."""
    if options_df.empty:
        return

    st.markdown(
        """
        <style>
        .timeline-card {background: linear-gradient(120deg,#0f172a 0%,#1f2937 40%,#0b1628 100%); color:#eef2ff;
            padding:16px 18px; border-radius:14px; margin-bottom:12px; box-shadow:0 12px 28px rgba(15,23,42,0.35);}
        .timeline-title {font-weight:700; margin-bottom:6px;}
        .timeline-row {margin:10px 0 16px 0;}
        .timeline-label {font-size:13px; color:#cbd5e1; margin-bottom:6px;}
        .timeline-bar {display:flex; border-radius:10px; overflow:visible; background:#0b1222; border:1px solid #1e293b; flex-wrap:wrap;}
        .timeline-segment {height:32px; display:flex; align-items:center; justify-content:center;
            font-size:12px; color:#e2e8f0; white-space:nowrap; position:relative; cursor:pointer;}
        .timeline-segment:not(:last-child) {border-right:1px solid rgba(255,255,255,0.08);}
        .timeline-badge {margin-left:8px; padding:3px 8px; border-radius:999px; font-size:11px; background:rgba(255,255,255,0.08);}
        .timeline-segment:hover {outline:1px solid rgba(255,255,255,0.25);}
        .timeline-segment[data-tip]:hover:after {
            content: attr(data-tip);
            position:absolute;
            bottom:110%;
            left:50%;
            transform:translateX(-50%);
            background:rgba(15,23,42,0.95);
            color:#e2e8f0;
            padding:6px 10px;
            border-radius:8px;
            font-size:11px;
            white-space:pre-line;
            word-break:break-word;
            max-width:260px;
            line-height:1.35;
            box-shadow:0 8px 18px rgba(0,0,0,0.4);
            z-index:10;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    palette = ["#1e293b", "#0ea5e9", "#0ea5e9", "#0284c7", "#0369a1", "#075985"]

    def color_for(count: int, max_count: int) -> str:
        if max_count <= 0:
            return palette[0]
        ratio = count / max_count
        idx = min(len(palette) - 1, int(ratio * (len(palette) - 1)))
        return palette[idx]

    day_groups = []
    for _, row in options_df.iterrows():
        start_dt = datetime.fromisoformat(row["start_ts"])
        end_dt = datetime.fromisoformat(row["end_ts"])
        key = start_dt.strftime("%Y-%m-%d (%a)")
        day_groups.append((key, start_dt, end_dt, row["option_id"]))

    grouped = {}
    for key, start_dt, end_dt, oid in day_groups:
        grouped.setdefault(key, []).append((start_dt, end_dt, oid))

    max_count_overall = max(summary_map.values()) if summary_map else 0

    st.markdown(
        f"<div class='timeline-card'><div class='timeline-title'>{t('timeline_title')}</div>",
        unsafe_allow_html=True,
    )
    for day_key, slots in grouped.items():
        slots = sorted(slots, key=lambda x: x[0])
        day_start = min(s[0] for s in slots)
        day_end = max(s[1] for s in slots)
        total_minutes = max(1, int((day_end - day_start).total_seconds() / 60))
        seg_html = ""
        for start_dt, end_dt, oid in slots:
            duration = max(1, int((end_dt - start_dt).total_seconds() / 60))
            width = duration / total_minutes * 100
            count = summary_map.get(oid, 0)
            voters = voters_map.get(oid, [])
            voter_list = "\n".join(voters[:15]) if voters else ""
            tooltip_lines = [
                f"{start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')}",
                f"{count} {t('people_suffix')}",
            ]
            if voter_list:
                tooltip_lines.append(voter_list)
            tooltip = "\n".join(tooltip_lines)
            tooltip_safe = html.escape(tooltip, quote=True)
            seg_html += (
                f"<div class='timeline-segment' style='width:{width:.2f}%;"
                f"background:{color_for(count, max_count_overall)}' data-tip='{tooltip_safe}' title='{tooltip_safe}'>"
                f"{start_dt.strftime('%H:%M')}</div>"
            )
        st.markdown(
            f"""
            <div class='timeline-row'>
              <div class='timeline-label'>{day_key}<span class='timeline-badge'>{t('timeline_slots_badge', count=len(slots))}</span></div>
              <div class='timeline-bar'>{seg_html}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "lang" not in st.session_state:
    st.session_state.lang = "ko"
if "mode" not in st.session_state:
    st.session_state.mode = "admin"

lang_label_map = {"ko": "한국어", "en": "English"}
st.sidebar.selectbox(
    t("language"),
    options=list(lang_label_map.keys()),
    format_func=lambda x: lang_label_map.get(x, x),
    key="lang",
)
mode_label_map = {"admin": t("mode_admin"), "guest": t("mode_guest")}
st.sidebar.radio(t("mode"), options=["admin", "guest"], format_func=lambda x: mode_label_map[x], key="mode")

st.title(t("app_title"))

admin_password = get_admin_password()

st.sidebar.info(t("sidebar_info"))

if st.session_state.mode == "admin":
    if not st.session_state.logged_in:
        with st.form("login"):
            password = st.text_input(t("login_title"), type="password")
            login = st.form_submit_button(t("login_button"))
        if login:
            if password == admin_password:
                st.session_state.logged_in = True
                st.success(t("login_success"))
                st.rerun()
            else:
                st.error(t("login_error"))
        st.stop()
else:
    st.session_state.logged_in = False  # ensure guest mode doesn't carry admin flag

conn = get_conn_cached()
for col in ("final_start_ts", "final_end_ts"):
    try:
        conn.execute(f"ALTER TABLE polls ADD COLUMN {col} TEXT")
    except Exception:
        pass
open_polls = load_polls(conn)

is_admin = st.session_state.mode == "admin" and st.session_state.logged_in

if is_admin:
    col_left, col_right = st.columns([1, 1.3])
else:
    col_left = None
    col_right = st.container()

with col_left if col_left else st.container():
    if is_admin:
        # initialize defaults once
        if "form_initialized" not in st.session_state:
            st.session_state["form_poll_id"] = ""
            st.session_state["form_title"] = t("create_title")
            st.session_state["form_desc"] = t("poll_desc_default")
            st.session_state["form_date_range"] = (date.today(), date.today() + timedelta(days=7))
            st.session_state["form_start_time"] = time(9, 0)
            st.session_state["form_end_time"] = time(18, 0)
            st.session_state["form_slot_minutes"] = 60
            st.session_state["form_poll_password"] = ""
            st.session_state["form_initialized"] = True

        # apply pending prefill before rendering widgets
        prefill = st.session_state.pop("prefill_data", None)
        if prefill:
            for k, v in prefill.items():
                st.session_state[k] = v

        st.subheader(t("create_title"))
        with st.form("create_poll"):
            poll_id = st.text_input(t("poll_id"), key="form_poll_id", help=t("poll_id_help"))
            title = st.text_input(t("poll_title"), key="form_title")
            description = st.text_area(t("poll_desc"), key="form_desc")
            date_range = st.date_input(
                t("date_range"),
                key="form_date_range",
                help=t("date_help"),
            )
            start_time = st.time_input(t("start_time"), key="form_start_time")
            end_time = st.time_input(t("end_time"), key="form_end_time")
            slot_minutes = st.number_input(t("slot_minutes"), min_value=15, max_value=240, step=15, key="form_slot_minutes")
            poll_password = st.text_input(t("poll_password"), key="form_poll_password", help=t("poll_password_help"), type="password")
            submitted = st.form_submit_button(t("create_submit"))

        if submitted:
            if not poll_id:
                st.error(t("poll_id_required"))
            elif len(date_range) != 2:
                st.error(t("date_required"))
            elif start_time >= end_time:
                st.error(t("time_invalid"))
            else:
                start_d, end_d = date_range
                slots = [
                    (datetime.fromisoformat(s), datetime.fromisoformat(e))
                    for s, e in st.session_state.get("custom_slots", [])
                ]
                if not slots:
                    slots = generate_slots(start_d, end_d, start_time, end_time, int(slot_minutes))
                if not slots:
                    st.error(t("no_slots"))
                else:
                    conn.execute("DELETE FROM votes WHERE poll_id = ?", (poll_id,))
                    conn.execute("DELETE FROM options WHERE poll_id = ?", (poll_id,))
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO polls(
                            poll_id, title, description, start_date, end_date,
                            start_time, end_time, slot_minutes, poll_password, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            poll_id,
                            title,
                            description,
                            start_d.isoformat(),
                            end_d.isoformat(),
                            start_time.isoformat(),
                            end_time.isoformat(),
                            int(slot_minutes),
                            poll_password or None,
                            datetime.utcnow().isoformat(),
                        ),
                    )
                    for start_ts, end_ts in slots:
                        conn.execute(
                            "INSERT INTO options(poll_id, start_ts, end_ts) VALUES (?, ?, ?)",
                            (poll_id, start_ts.isoformat(), end_ts.isoformat()),
                    )
                conn.commit()
                st.success(t("poll_ready", poll_id=poll_id))
                open_polls = load_polls(conn)

        # interactive slot selection preview
        if len(date_range) == 2:
            start_d, end_d = date_range
            preview_slots = generate_slots(start_d, end_d, start_time, end_time, int(slot_minutes))
            sig = f"{start_d.isoformat()}|{end_d.isoformat()}|{start_time.isoformat()}|{end_time.isoformat()}|{slot_minutes}"
            if st.session_state.get("slot_sig") != sig:
                st.session_state["slot_sig"] = sig
                st.session_state["custom_slots"] = [(s.isoformat(), e.isoformat()) for s, e in preview_slots]
            st.markdown("_가능한 슬롯을 클릭해 선택/해제하세요 (미선택 시 전부 사용)_")
            grouped = {}
            for s, e in preview_slots:
                key = s.strftime("%Y-%m-%d (%a)")
                grouped.setdefault(key, []).append((s, e))
            for day, rows in grouped.items():
                st.markdown(f"**{day}**")
                cols = st.columns(min(4, len(rows)))
                for idx, (s, e) in enumerate(rows):
                    col = cols[idx % len(cols)]
                    with col:
                        key_val = (s.isoformat(), e.isoformat())
                        selected = key_val in st.session_state.get("custom_slots", [])
                        label = f"{s.strftime('%H:%M')} - {e.strftime('%H:%M')}"
                        btn_label = f"{'✅' if selected else '⬜️'} {label}"

                        def toggle_admin_slot(val=key_val):
                            cur = set(st.session_state.get("custom_slots", []))
                            if val in cur:
                                cur.remove(val)
                            else:
                                cur.add(val)
                            st.session_state["custom_slots"] = list(cur)

                        st.button(btn_label, key=f"admin-slot-{key_val}", on_click=toggle_admin_slot, use_container_width=True)

with col_right:
    st.subheader(t("participation") if not is_admin else t("open_polls"))
    if open_polls.empty:
        st.info(t("no_polls"))
        st.stop()

    poll_lookup = {row.poll_id: row.title for _, row in open_polls.iterrows()}
    selected_poll = st.selectbox(
        t("open_polls"),
        open_polls["poll_id"],
        format_func=lambda pid: f"{pid} · {poll_lookup.get(pid, '')}",
    )

    if selected_poll:
        poll_meta = pd.read_sql("SELECT * FROM polls WHERE poll_id = ?", conn, params=(selected_poll,)).iloc[0]
        st.caption(poll_meta["description"])
        poll_pw_required = poll_meta.get("poll_password")
        poll_pw_key = f"poll_pw_{selected_poll}"
        if poll_pw_key not in st.session_state:
            st.session_state[poll_pw_key] = ""

        options_df = pd.read_sql(
            "SELECT option_id, start_ts, end_ts FROM options WHERE poll_id = ? ORDER BY start_ts",
            conn,
            params=(selected_poll,),
        )

        votes_df = pd.read_sql(
            """
            SELECT v.voter_name, v.option_id, v.available, o.start_ts, o.end_ts
            FROM votes v
            JOIN options o ON v.option_id = o.option_id
            WHERE v.poll_id = ?
            """,
            conn,
            params=(selected_poll,),
        )

        votes_df_full = votes_df.copy()
        summary_map_full = votes_df_full.groupby("option_id")["available"].sum().to_dict() if not votes_df_full.empty else {}
        voters_map_full = (
            votes_df_full[votes_df_full["available"] == 1].groupby("option_id")["voter_name"].apply(list).to_dict()
            if not votes_df_full.empty
            else {}
        )

        summary_map = summary_map_full
        voters_map = voters_map_full
        st.caption(t("timeline_hint"))
        voter_filter = []
        if not votes_df_full.empty:
            voter_filter = st.multiselect("참여자 필터", sorted(votes_df_full["voter_name"].unique()))
            if voter_filter:
                filtered_votes = votes_df_full[votes_df_full["voter_name"].isin(voter_filter)]
                summary_map = filtered_votes.groupby("option_id")["available"].sum().to_dict()
                voters_map = (
                    filtered_votes[filtered_votes["available"] == 1]
                    .groupby("option_id")["voter_name"]
                    .apply(list)
                    .to_dict()
                    if not filtered_votes.empty
                    else {}
                )
        render_timeline(options_df, summary_map, voters_map)

        if is_admin:
            if st.button(t("edit_load"), type="secondary"):
                st.session_state["prefill_data"] = {
                    "form_poll_id": poll_meta["poll_id"],
                    "form_title": poll_meta["title"],
                    "form_desc": poll_meta["description"],
                    "form_date_range": (
                        datetime.fromisoformat(poll_meta["start_date"]).date(),
                        datetime.fromisoformat(poll_meta["end_date"]).date(),
                    ),
                    "form_start_time": time.fromisoformat(poll_meta["start_time"]),
                    "form_end_time": time.fromisoformat(poll_meta["end_time"]),
                    "form_slot_minutes": int(poll_meta["slot_minutes"]),
                    "form_poll_password": poll_meta.get("poll_password") or "",
                }
                st.rerun()

            st.markdown("###")
            with st.expander(t("delete_poll")):
                confirm = st.text_input(t("delete_confirm"), key=f"confirm_{selected_poll}")
                if st.button(t("delete_poll"), type="primary"):
                    if confirm != selected_poll:
                        st.error(t("delete_confirm"))
                    else:
                        try:
                            conn.execute("DELETE FROM votes WHERE poll_id = ?", (selected_poll,))
                            conn.execute("DELETE FROM options WHERE poll_id = ?", (selected_poll,))
                            conn.execute("DELETE FROM polls WHERE poll_id = ?", (selected_poll,))
                            conn.commit()
                            st.success(t("delete_done"))
                            st.rerun()
                        except Exception as exc:
                            st.error(t("delete_fail", error=exc))

        access_granted = True if is_admin else not poll_pw_required
        first_chosen = None

        if not is_admin:
            if poll_pw_required and not access_granted:
                pw_input = st.text_input(t("poll_password_prompt"), type="password", key=poll_pw_key)
                if pw_input == poll_pw_required:
                    access_granted = True
                else:
                    st.info(t("access_needed"))

            if not access_granted:
                st.stop()

            st.markdown(f"**{t('vote_section')}**")
            sel_key = f"selections_{selected_poll}"
            st.session_state.setdefault(sel_key, [])
            voter_name = st.text_input(t("voter_name"), max_chars=40, key=f"name_{selected_poll}")
            voter_pw = st.text_input(t("voter_pw"), type="password", key=f"pw_{selected_poll}", help=t("voter_pw_help"))
            comment = st.text_input(t("comment"), max_chars=120, key=f"comment_{selected_poll}")

            def load_my_vote():
                if not voter_name:
                    return
                if not voter_pw:
                    st.error(t("voter_pw_need"))
                    return
                existing = pd.read_sql(
                    "SELECT option_id, available, comment, voter_password FROM votes WHERE poll_id = ? AND voter_name = ?",
                    conn,
                    params=(selected_poll, voter_name),
                )
                if not existing.empty:
                    stored_pw = existing["voter_password"].dropna()
                    if not stored_pw.empty and stored_pw.iloc[0] and stored_pw.iloc[0] != voter_pw:
                        st.error(t("voter_pw_mismatch"))
                        return
                    # If previous record had no password, claim it by setting now
                    if stored_pw.empty or not stored_pw.iloc[0]:
                        conn.execute(
                            "UPDATE votes SET voter_password = ? WHERE poll_id = ? AND voter_name = ?",
                            (voter_pw, selected_poll, voter_name),
                        )
                        conn.commit()
                    chosen = existing[existing["available"] == 1]["option_id"].tolist()
                    st.session_state[sel_key] = chosen
                    first_comment = existing["comment"].dropna()
                    if not first_comment.empty:
                        st.session_state[f"comment_{selected_poll}"] = first_comment.iloc[0]

            st.button(t("load_my_vote"), on_click=load_my_vote)

            grouped_slots = {}
            for _, opt in options_df.iterrows():
                start_dt = datetime.fromisoformat(opt["start_ts"])
                key = start_dt.strftime("%Y-%m-%d (%a)")
                grouped_slots.setdefault(key, []).append(opt)

            st.markdown(f"_{t('choose_slots')}_")
            st.markdown(
                """
                <style>
                .slot-button button {width:100%; margin-bottom:6px; border-radius:10px;}
                @media (max-width: 768px) {.timeline-bar{flex-wrap:wrap;} .timeline-segment{min-width:80px; flex:1 1 45%;}}
                </style>
                """,
                unsafe_allow_html=True,
            )
            for day, rows in grouped_slots.items():
                st.markdown(f"**{day}**")
                cols = st.columns(min(4, len(rows)))
                for idx, opt in enumerate(rows):
                    col = cols[idx % len(cols)]
                    with col:
                        selected = opt["option_id"] in st.session_state[sel_key]
                        label = slot_label(opt["start_ts"], opt["end_ts"])
                        btn_label = f"{'✅' if selected else '⬜️'} {label}"

                        def toggle_option(oid=opt["option_id"]):
                            current = set(st.session_state.get(sel_key, []))
                            if oid in current:
                                current.remove(oid)
                            else:
                                current.add(oid)
                            st.session_state[sel_key] = list(current)

                        st.button(btn_label, key=f"btn-{opt['option_id']}", on_click=toggle_option, type="secondary", help=label, use_container_width=True)
            st.caption(t("selected_count", count=len(st.session_state[sel_key])))

            if st.session_state[sel_key]:
                first_opt = options_df[options_df["option_id"] == st.session_state[sel_key][0]].iloc[0]
                first_chosen = (
                    datetime.fromisoformat(first_opt["start_ts"]),
                    datetime.fromisoformat(first_opt["end_ts"]),
                )

            if st.button(t("vote_submit"), type="primary"):
                if not voter_name:
                    st.error(t("name_required"))
                elif not voter_pw:
                    st.error(t("voter_pw_need"))
                else:
                    chosen = set(st.session_state.get(sel_key, []))
                    for _, opt in options_df.iterrows():
                        available = int(opt["option_id"] in chosen)
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO votes(poll_id, voter_name, option_id, available, comment, voter_password)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (selected_poll, voter_name, opt["option_id"], available, comment, voter_pw),
                        )
                    conn.commit()
                    st.success(t("vote_saved"))
                    st.rerun()

        summary = pd.DataFrame()

        best_slot = None
        final_slot = None
        if not votes_df_full.empty:
            options_df["label"] = options_df.apply(lambda row: slot_label(row["start_ts"], row["end_ts"]), axis=1)
            chosen = votes_df_full[votes_df_full["available"] == 1].merge(
                options_df[["option_id", "label"]], on="option_id", how="left"
            )
            per_voter = (
                chosen.groupby("voter_name")["label"].apply(lambda x: ", ".join(sorted(set(x)))).reset_index()
            )
            st.markdown(f"**{t('selected_slots')}**")
            st.dataframe(per_voter.rename(columns={"voter_name": t("voter_name"), "label": t("popular_col_slot")}))

            per_slot = (
                chosen.groupby(["option_id", "label"])["voter_name"]
                .apply(lambda x: ", ".join(sorted(set(x))))
                .reset_index()
            )
            per_slot = per_slot.sort_values(by="label")
            st.markdown(f"**{t('popular_names')}**")
            st.dataframe(per_slot.rename(columns={"label": t("popular_col_slot"), "voter_name": t("voter_name")}))
            if summary_map_full:
                best_id = max(summary_map_full.items(), key=lambda kv: kv[1])[0]
                best_slot_row = options_df[options_df["option_id"] == best_id].iloc[0]
                best_slot = (
                    datetime.fromisoformat(best_slot_row["start_ts"]),
                    datetime.fromisoformat(best_slot_row["end_ts"]),
                )
            if poll_meta.get("final_start_ts") and poll_meta.get("final_end_ts"):
                final_slot = (
                    datetime.fromisoformat(poll_meta["final_start_ts"]),
                    datetime.fromisoformat(poll_meta["final_end_ts"]),
                )
        else:
            st.info(t("no_responses"))

        st.markdown("---")
        if is_admin:
            total_voters = votes_df_full["voter_name"].nunique() if not votes_df_full.empty else 0
            if not votes_df_full.empty:
                st.markdown("**Finalize schedule**")
                option_counts = votes_df_full[votes_df_full["available"] == 1].groupby("option_id")["voter_name"].nunique()
                option_counts = option_counts.reindex(options_df["option_id"]).fillna(0).astype(int)
                finalize_options = []
                for _, row in options_df.iterrows():
                    oid = row["option_id"]
                    label = slot_label(row["start_ts"], row["end_ts"])
                    cnt = option_counts.get(oid, 0)
                    tag = " ✅" if total_voters > 0 and cnt == total_voters else ""
                    finalize_options.append((oid, f"{label} · {cnt}/{total_voters}{tag}"))
                default_index = 0
                if best_slot:
                    best_id = max(summary_map.items(), key=lambda kv: kv[1])[0]
                    for idx, (oid, _) in enumerate(finalize_options):
                        if oid == best_id:
                            default_index = idx
                            break
                chosen_oid = st.selectbox(
                    "확정할 슬롯",
                    finalize_options,
                    format_func=lambda x: x[1],
                    index=default_index,
                    key=f"finalize_{selected_poll}",
                )
                if st.button("확정하기"):
                    oid = chosen_oid[0] if isinstance(chosen_oid, tuple) else chosen_oid
                    row = options_df[options_df["option_id"] == oid].iloc[0]
                    conn.execute(
                        "UPDATE polls SET final_start_ts = ?, final_end_ts = ? WHERE poll_id = ?",
                        (row["start_ts"], row["end_ts"], selected_poll),
                    )
                    conn.commit()
                    st.success("일정이 확정되었습니다.")
                    st.session_state["finalized_slot"] = (row["start_ts"], row["end_ts"])
                    st.rerun()
                if final_slot or st.session_state.get("finalized_slot"):
                    fs = st.session_state.get("finalized_slot")
                    if fs:
                        final_slot = (datetime.fromisoformat(fs[0]), datetime.fromisoformat(fs[1]))
                    if final_slot:
                        label = f"{final_slot[0].strftime('%Y-%m-%d %H:%M')} - {final_slot[1].strftime('%H:%M')}"
                        st.info(f"확정된 슬롯: {label}")
                        email_subject = f"[Schedule Confirmed] {poll_meta['title']}"
                        email_body = "\n".join(
                            [
                                f"{poll_meta['title']} 일정이 확정되었습니다.",
                                f"시간: {label}",
                                "장소/링크: ",
                                f"참여자: {', '.join(sorted(votes_df_full['voter_name'].unique()))}",
                                f"비고: {poll_meta['description']}",
                            ]
                        )
                        st.text_area("메일 초안", value=f"Subject: {email_subject}\n\n{email_body}", height=140)
            st.markdown("**Synology Calendar**")
            syno_url = st.text_input(t("syno_url"), key="syno_url", placeholder="https://your-nas:5001")
            syno_user = st.text_input(t("syno_user"), key="syno_user")
            syno_pass = st.text_input(t("syno_pass"), key="syno_pass", type="password")
            syno_cal = st.text_input(t("syno_cal"), key="syno_cal", help="예: personal calendar UUID or name")
            if st.button(t("syno_upload")):
                if not best_slot:
                    st.error(t("syno_need_slot"))
                elif not all([syno_url, syno_user, syno_pass, syno_cal]):
                    st.error("URL/계정/비밀번호/캘린더 ID를 입력하세요.")
                else:
                    try:
                        slot_for_sync = final_slot or best_slot
                        push_synology_event(
                            syno_url.rstrip("/"),
                            syno_user,
                            syno_pass,
                            syno_cal,
                            poll_meta["title"],
                            poll_meta["description"],
                            slot_for_sync[0],
                            slot_for_sync[1],
                        )
                        st.success(t("syno_success"))
                    except Exception as exc:
                        st.error(t("syno_error", error=exc))
        else:
            st.markdown(f"**{t('add_calendar')}**")
            active_slot = final_slot
            if not active_slot:
                st.info("관리자가 확정한 시간대가 있어야 내보낼 수 있습니다.")
            else:
                start_dt, end_dt = active_slot
                ics = build_ics(start_dt, end_dt, poll_meta["title"], poll_meta["description"])
                st.download_button(t("ical_download"), data=ics, file_name=f"{selected_poll}.ics", mime="text/calendar")
                fmt = "%Y%m%dT%H%M%S"
                gstart = start_dt.strftime(fmt)
                gend = end_dt.strftime(fmt)
                g_url = (
                    "https://calendar.google.com/calendar/render?"
                    f"action=TEMPLATE&text={poll_meta['title']}"
                    f"&details={poll_meta['description']}"
                    f"&dates={gstart}/{gend}"
                )
                st.link_button(t("google_add"), g_url, type="primary")
