"""
══════════════════════════════════════════════════════════════════════
LOCUST LOAD TEST — Examly API  (Full Exam Lifecycle)
══════════════════════════════════════════════════════════════════════

Flow per virtual user (single @task):
  1. GET  /start
  ── 3-minute loop ────────────────────────────
  2. POST /event/option-click      (select an MCQ answer)
  3. POST /event/clear-answer      (clear the selected answer)
  4. POST /event-history           (batched UI events; QUESTION_ID_1 + Java answer[] from config)
  5. POST /event/heart-beat        (empty answer — after clear)
  6. POST /question/{id}/compile   (Compiled & Run)
  7. POST /event/heart-beat        (with code — after compile)
  ──────────────────────────────────────────────────────────────────
  8. POST /submit                  (final submission)

Usage:
  locust -f locustfile.py --host https://api.iamneo.ai

Config:
  JWT tokens are auto-generated in config.py for all 100 users.
══════════════════════════════════════════════════════════════════════
"""

import random
import time
import threading
from datetime import datetime, timezone, timedelta
from locust import HttpUser, task, between, events
from locust.exception import RescheduleTask, StopUser
from config import SCENARIOS, HEADERS_TEMPLATE, QUESTION_ID_1


# ── Shared response tracker ────────────────────────────────────────

stats = {"success": 0, "error": 0, "timeout": 0}
scenario_lock = threading.Lock()
available_scenarios = []


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    if exception:
        stats["error"] += 1
    else:
        stats["success"] += 1


# ── Helpers ────────────────────────────────────────────────────────

def _now_ts() -> str:
    """Return current UTC time as ISO-8601 with milliseconds e.g. 2026-04-13T08:27:22.389Z"""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _utc_ts_offset_ms(offset_ms: int = 0) -> str:
    """UTC ISO-8601 timestamp shifted by offset_ms (for ordered batched event-history)."""
    now = datetime.now(timezone.utc) + timedelta(milliseconds=offset_ms)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


JAVA_CODE = (
    'import java.util.Scanner;\n'
    'public class Main {\n'
    '    public static void main(String[] args) {\n'
    '        Scanner sc = new Scanner(System.in);\n'
    '        int a = sc.nextInt();\n'
    '        int b = sc.nextInt();\n'
    '        System.out.println("Sum of x + y = " + (a + b));\n'
    '    }\n'
    '}'
)

# SIT MCQ Answer
MCQ_ANSWER = MCQ_ANSWER = ["<p>12</p>", "<p>18</p>", "<p>36</p>"]
MCQ_Q_TYPE  = "mcq_multiple_correct"


# MCQ question IDs from the frozen exam (option-click / clear-answer targets)
MCQ_QUESTION_IDS = [
    # SIT
    "69bacc125b8a24ee8d3bef4a"
]

LOOP_DURATION_SEC  = 300
LOOP_INTERVAL_SEC  = 5



# One-liner shared headers for same-site endpoints (origin differs from /start)
def _same_site_headers(token: str) -> dict:
    return {
        **HEADERS_TEMPLATE,
        "Authorization":   f"Bearer {token}",
        "Origin":          "https://api.iamneo.ai",
        "Referer":         "https://api.iamneo.ai/",
        "Priority":        "u=1, i",
        "Sec-Ch-Ua":       '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "Sec-Ch-Ua-Mobile":   "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Site":  "same-site",
    }


# ══════════════════════════════════════════════════════════════════
# BASE USER CLASS — all endpoint helpers live here
# ══════════════════════════════════════════════════════════════════

class ExamBaseUser(HttpUser):
    abstract = True

    def on_start(self):
        """Assign one unique scenario/user on spawn and initialize flow state."""
        self.session_started = False
        self.session_submitted = False

        with scenario_lock:
            if not available_scenarios:
                print("No available scenarios left. Check users vs SCENARIOS length.")
                raise RescheduleTask()
            self.scenario = available_scenarios.pop()
        self._event_hist_seq = 0

    # ── 1. /start ─────────────────────────────────────────────────
    def _start(self) -> bool:
        """GET /start — must succeed before any other call."""
        url = (
            f"/api/v2/assessment/courses/{self.scenario['course_id']}"
            f"/tests/{self.scenario['test_id']}/start"
        )
        headers = {
            "Accept":           "application/json, text/plain, */*",
            "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
            "Authorization":    f"Bearer {self.scenario['token']}",
            "Priority":         "u=1, i",
            "Sec-Ch-Ua":        '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            "Sec-Ch-Ua-Mobile":    "?0",
            "Sec-Ch-Ua-Platform":  '"macOS"',
            "Sec-Fetch-Dest":   "empty",
            "Sec-Fetch-Mode":   "cors",
            "Sec-Fetch-Site":   "same-site",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }
        with self.client.get(
            url, headers=headers, name="/start",
            catch_response=True, timeout=60,
        ) as r:
            print(f"[START] user={self.scenario['user_id']} status={r.status_code} body={r.text[:200]}")
            if r.status_code in (200, 201, 409):
                r.success()
                return True
            req_id = r.headers.get("x-request-id", r.headers.get("cf-ray", "unknown"))
            r.failure(f"req_id={req_id} HTTP {r.status_code}")
            return False

    # ── 2. /event/option-click ────────────────────────────────────
    def _option_click(self, q_id: str, answer: str = MCQ_ANSWER, q_type: str = MCQ_Q_TYPE):
        """POST /event/option-click — simulates the student clicking an MCQ choice."""
        url = (
            f"/api/v2/assessment/courses/{self.scenario['course_id']}"
            f"/tests/{self.scenario['test_id']}/event/option-click"
        )
        payload = {
            "question_id":   q_id,
            "question_type": q_type,
            "answer":        answer,
            "timestamp":     _now_ts(),
        }
        with self.client.post(
            url, json=payload,
            headers=_same_site_headers(self.scenario["token"]),
            name="/event/option-click", catch_response=True, timeout=60,
        ) as r:
            if r.status_code in (200, 201):
                r.success()
            else:
                req_id = r.headers.get("x-request-id", r.headers.get("cf-ray", "unknown"))
                r.failure(f"req_id={req_id} HTTP {r.status_code}: {r.text[:200]}")

    # ── 3. /event/clear-answer ────────────────────────────────────
    def _clear_answer(self, q_id: str, q_type: str = MCQ_Q_TYPE):
        """POST /event/clear-answer — clears the previously selected MCQ answer."""
        url = (
            f"/api/v2/assessment/courses/{self.scenario['course_id']}"
            f"/tests/{self.scenario['test_id']}/event/clear-answer"
        )
        payload = {
            "question_id":   q_id,
            "question_type": q_type,
            "answer":        "",        # empty string = cleared
            "timestamp":     _now_ts(),
        }
        with self.client.post(
            url, json=payload,
            headers=_same_site_headers(self.scenario["token"]),
            name="/event/clear-answer", catch_response=True, timeout=60,
        ) as r:
            if r.status_code in (200, 201):
                r.success()
            else:
                req_id = r.headers.get("x-request-id", r.headers.get("cf-ray", "unknown"))
                r.failure(f"req_id={req_id} HTTP {r.status_code}: {r.text[:200]}")

    # ── 4. /event-history ─────────────────────────────────────────
    def _event_history(self):
        """POST /event-history — programming payload shape from learner UI (config QUESTION_ID_1)."""
        url = (
            f"/api/v2/assessment/courses/{self.scenario['course_id']}"
            f"/tests/{self.scenario['test_id']}/event-history"
        )
        q_id = QUESTION_ID_1
        q_type = "programming"
        ua = HEADERS_TEMPLATE["User-Agent"]
        screen_w = 1352
        browser = {"userAgent": ua, "screenWidth": screen_w}

        def next_id() -> int:
            self._event_hist_seq += 1
            return self._event_hist_seq

        def prog_event_data(code: str) -> dict:
            """event_data.answer is a list of {language, code} (same as browser capture)."""
            return {
                "answer": [{"language": "Java", "code": code}],
                "browser_data": browser,
            }

        # Default batch: same question_id (QUESTION_ID_1), Java static code then empty editor states.
        events = [
            {
                "question_id": q_id,
                "question_type": q_type,
                "event_type": "skipped",
                "event_timestamp": _utc_ts_offset_ms(0),
                "event_data": prog_event_data(JAVA_CODE),
                "id": next_id(),
            },
            {
                "question_id": q_id,
                "question_type": q_type,
                "event_type": "book-marked",
                "event_timestamp": _utc_ts_offset_ms(400),
                "event_data": prog_event_data(""),
                "id": next_id(),
            },
            {
                "question_id": q_id,
                "question_type": q_type,
                "event_type": "skipped",
                "event_timestamp": _utc_ts_offset_ms(900),
                "event_data": prog_event_data(""),
                "id": next_id(),
            },
        ]
        headers = {
            **_same_site_headers(self.scenario["token"]),
            "Content-Type": "application/json",
        }
        with self.client.post(
            url, json={"events": events},
            headers=headers,
            name="/event-history", catch_response=True, timeout=60,
        ) as r:
            req_id = r.headers.get("x-request-id", r.headers.get("cf-ray", "unknown"))
            if r.status_code in (200, 201):
                r.success()
            elif r.status_code == 202:
                try:
                    body = r.json()
                    if body.get("status") == "accepted":
                        r.success()
                    else:
                        r.failure(
                            f"req_id={req_id} HTTP 202 unexpected body: {r.text[:200]}"
                        )
                except Exception:
                    r.failure(f"req_id={req_id} HTTP 202 non-JSON: {r.text[:200]}")
            else:
                r.failure(f"req_id={req_id} HTTP {r.status_code}: {r.text[:200]}")

    # ── 5 & 7. /event/heart-beat ─────────────────────────────────
    def _heartbeat(self, code: str = "", language: str = "Java"):
        """POST /event/heart-beat — sends current code/answer state."""
        url = (
            f"/api/v2/assessment/courses/{self.scenario['course_id']}"
            f"/tests/{self.scenario['test_id']}/event/heart-beat"
        )
        payload = {
            "question_id":   self.scenario["question_id"],
            "question_type": "programming",
            "answer":        {"language": language, "code": code},
            "timestamp":     _now_ts(),
        }
        with self.client.post(
            url, json=payload,
            headers=_same_site_headers(self.scenario["token"]),
            name="/event/heart-beat", catch_response=True, timeout=60,
        ) as r:
            if r.status_code in (200, 201):
                r.success()
            else:
                req_id = r.headers.get("x-request-id", r.headers.get("cf-ray", "unknown"))
                r.failure(f"req_id={req_id} HTTP {r.status_code}: {r.text[:200]}")

    # ── 6. /question/{id}/compile ─────────────────────────────────
    def _compile(self, code: str, language: str = "Java",
                 event_type: str = "Compiled & Run", custom_input: str = ""):
        """POST /compile — submit code for execution."""
        url = (
            f"/api/v2/assessment/courses/{self.scenario['course_id']}"
            f"/tests/{self.scenario['test_id']}"
            f"/question/{self.scenario['question_id']}/compile"
        )
        headers = {
            **_same_site_headers(self.scenario["token"]),
            "Content-Type": "application/json",
        }
        payload = {
            "code":         code,
            "language":     language,
            "event_type":   event_type,
            "custom_input": custom_input,
        }
        with self.client.post(
            url, json=payload, headers=headers,
            name=f"/compile [{language}]", catch_response=True, timeout=60
        ) as r:
            req_id = r.headers.get("x-request-id", r.headers.get("cf-ray", "unknown"))
            if r.status_code == 200:
                try:
                    body = r.json()
                    if any(k in body for k in ["output", "result", "status", "data", "job_id", "message"]):
                        r.success()
                    else:
                        r.failure(f"req_id={req_id} Unexpected shape: {list(body.keys())}")
                except Exception:
                    r.success()
            elif r.status_code in (401, 403):
                r.failure(f"req_id={req_id} Auth error {r.status_code}: {r.text[:200]}")
                raise RescheduleTask()
            elif r.status_code == 429:
                stats["timeout"] += 1
                r.failure(f"req_id={req_id} 429 Too Many Requests")
                time.sleep(5)
                raise RescheduleTask()
            else:
                r.failure(f"req_id={req_id} HTTP {r.status_code}: {r.text[:200]}")

    # ── 8. /submit ────────────────────────────────────────────────
    def _submit_test(self):
        """POST /submit — final exam submission."""
        url = (
            f"/api/v2/assessment/courses/{self.scenario['course_id']}"
            f"/tests/{self.scenario['test_id']}/submit"
        )
        payload = {
            "frozen_data_id": "69da0a00c5729313f1195657",
            "questions": [
                {
                    "q_id":   self.scenario["question_id"],
                    "answer": ""
                }
            ],
        }
        with self.client.post(
            url, json=payload,
            headers=_same_site_headers(self.scenario["token"]),
            name="/submit", catch_response=True, timeout=60,
        ) as r:
            if r.status_code in (200, 201, 202, 204):
                r.success()
                print(f"[SUBMIT] ✓ user={self.scenario['user_id']} submitted OK")
            else:
                req_id = r.headers.get("x-request-id", r.headers.get("cf-ray", "unknown"))
                r.failure(f"req_id={req_id} HTTP {r.status_code}: {r.text[:200]}")


class ExamUser(ExamBaseUser):
    """
    Simulates a complete student exam session:

      /start
        option-click → clear-answer → event-history → heart-beat → compile → heart-beat
      ─────────────────────────────────────────────────────────────
      /submit
    """
    wait_time = between(1, 3)
    weight    = 1

    @task
    def full_exam_flow(self):
        uid = self.scenario["user_id"]

        # Safety guard: don't run any test activity after submit.
        if self.session_submitted:
            raise StopUser()

        # ── Step 1: /start ────────────────────────────────────────
        if not self.session_started and not self._start():
            print(f"[FLOW] ✗ /start failed for user={uid} — aborting")
            return
        self.session_started = True

        print(f"[FLOW] ✓ Started exam session for user={uid}")

        # ── Steps 2-6: 3-minute activity loop ────────────────────
        loop_start = time.time()
        iteration  = 0

        while (time.time() - loop_start) < LOOP_DURATION_SEC:
            iteration += 1
            elapsed = int(time.time() - loop_start)
            print(f"[FLOW] user={uid} loop iter={iteration} elapsed={elapsed}s")

            # Pick a random MCQ question for option-click / clear-answer
            q_id = random.choice(MCQ_QUESTION_IDS)

            # 2. option-click
            print(f"[FLOW] → option-click  q={q_id}")
            self._option_click(q_id=q_id)

            # # 3. clear-answer
            print(f"[FLOW] → clear-answer  q={q_id}")
            self._clear_answer(q_id=q_id)

            # # 4. event-history (batched; uses scenario question_id from config)
            print(f"[FLOW] → event-history  q={QUESTION_ID_1} (config)")
            self._event_history()

            # # 5. heart-beat (empty — answer cleared)
            print(f"[FLOW] → heart-beat (empty)")
            self._heartbeat(code="", language="Java")

            # # 6. compile
            print(f"[FLOW] → compile")
            self._compile(code=JAVA_CODE, language="Java", event_type="Compiled & Run")

            # # 7. heart-beat (with code — after compile)
            print(f"[FLOW] → heart-beat (with code)")
            self._heartbeat(code=JAVA_CODE, language="Java")

            # Wait before next iteration (unless we've already exceeded the window)
            remaining = LOOP_DURATION_SEC - (time.time() - loop_start)
            if remaining > 0:
                sleep_time = min(LOOP_INTERVAL_SEC, remaining)
                print(f"[FLOW] Sleeping {sleep_time:.0f}s before next iteration...")
                time.sleep(sleep_time)

        # ── Step 8: /submit ───────────────────────────────────────
        print(f"[FLOW] {LOOP_DURATION_SEC} sec loop done ({iteration} iters) — submitting for user={uid}")
        self._submit_test()
        self.session_submitted = True
        
        # Stop user after one successful full loop so it won't restart
        print(f"[FLOW] ✓ Session complete for user={uid}. Stopping user.")
        raise StopUser()


# ══════════════════════════════════════════════════════════════════
# LIFECYCLE HOOKS
# ══════════════════════════════════════════════════════════════════

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global available_scenarios
    with scenario_lock:
        available_scenarios = SCENARIOS.copy()
        random.shuffle(available_scenarios)

    print("\n" + "="*60)
    print("  🚀 Locust Load Test — Examly Full Exam Lifecycle")
    print(f"  Target     : {environment.host}")
    print(f"  Scenarios  : {len(SCENARIOS)} users")
    print(f"  Loop time  : {LOOP_DURATION_SEC}s  |  interval: {LOOP_INTERVAL_SEC}s")
    print("  Flow: /start → [option-click→clear-answer→event-history→hb→compile→hb] × N → /submit")
    print("="*60 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("\n" + "="*60)
    print("  📊 Test Summary")
    print(f"  ✓ Success  : {stats['success']}")
    print(f"  ✗ Error    : {stats['error']}")
    print(f"  ⏱ Timeout  : {stats['timeout']}")
    print("="*60 + "\n")
