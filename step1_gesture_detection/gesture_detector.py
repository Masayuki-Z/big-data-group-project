"""
gesture_detector.py
--------------------
Step 1 – Data Source Layer  (v3 – new gesture set)

Gesture → Priority mapping:
  unknown  → priority 0  (no / unrecognised gesture)
  low_sign → priority 1  LEFT hand only:
                          index pointing UP + thumb lying horizontal,
                          middle / ring / pinky folded
  medium_sign → priority 2  Either hand:
                          index + middle + ring pointing DOWN (like 'M'),
                          thumb & pinky position ignored
  high_sign  → priority 3  Either hand (Thai love / ILY):
                          index UP + pinky UP + thumb horizontal,
                          middle + ring folded

NOTE on handedness after mirror-flip:
  We flip the frame with cv2.flip(frame,1) before showing it (natural
  mirror view).  MediaPipe processes the flipped image, so its labels
  are swapped relative to the user's actual hands:
      MediaPipe "Right"  →  user's LEFT  hand
      MediaPipe "Left"   →  user's RIGHT hand
  The LOW gesture checks for MediaPipe label "Right".
"""

import cv2
import threading
import mediapipe.python.solutions.hands as mp_hands_module
import mediapipe.python.solutions.drawing_utils as mp_drawing_module
import mediapipe.python.solutions.drawing_styles as mp_drawing_styles_module
from datetime import datetime
from time import time


# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
CONFIRM_SECONDS  = 3.0
COOLDOWN_SECONDS = 3.0

GESTURE_MAP = {
    "low_sign":    {"danger_level": "LOW",      "priority": 1},
    "medium_sign": {"danger_level": "MEDIUM",   "priority": 2},
    "high_sign":   {"danger_level": "HIGH",     "priority": 3},
    "unknown":     {"danger_level": "UNKNOWN",  "priority": 0},
}

LEVEL_COLORS = {
    "LOW":     (0,   200,   0),    # green
    "MEDIUM":  (0,   165, 255),    # orange
    "HIGH":    (0,     0, 255),    # red
    "UNKNOWN": (80,   80,  80),    # grey
}


# ──────────────────────────────────────────────
# Gesture Classifier
# ──────────────────────────────────────────────
class GestureClassifier:
    """
    Classifies hand landmarks + handedness into one of the four gestures.

    Coordinate system (MediaPipe normalised):
      x : 0 (left edge) → 1 (right edge)
      y : 0 (top edge)  → 1 (bottom edge)   ← y increases DOWNWARD
      z : depth (not used here)

    Key rules used:
      Finger pointing UP   → tip.y < pip.y   (tip is higher on screen)
      Finger pointing DOWN → tip.y > pip.y   AND tip.y > wrist.y
      Thumb horizontal     → |thumb_tip.y - wrist.y| < THUMB_H_TOL
                             AND distance(thumb_tip, wrist) > THUMB_EXTEND_MIN
      Finger folded        → tip.y > pip.y   (tip below its own knuckle)
    """

    # Landmark indices
    WRIST      = 0
    THUMB_TIP  = 4;  THUMB_IP   = 3;  THUMB_MCP = 2
    INDEX_TIP  = 8;  INDEX_PIP  = 6
    MIDDLE_TIP = 12; MIDDLE_PIP = 10
    RING_TIP   = 16; RING_PIP   = 14
    PINKY_TIP  = 20; PINKY_PIP  = 18

    # Tuning thresholds (normalised 0-1 space)
    THUMB_H_TOL      = 0.10   # max |y_thumb_tip - y_wrist| to call thumb "horizontal"
    THUMB_EXTEND_MIN = 0.08   # min distance wrist→thumb_tip to confirm thumb is out

    # ── helpers ─────────────────────────────────

    def _dist(self, a, b):
        return ((a.x - b.x) ** 2 + (a.y - b.y) ** 2) ** 0.5

    def _tip_above_pip(self, lm, tip_i, pip_i) -> bool:
        """Finger pointing UP: tip is higher than PIP (smaller y)."""
        return lm[tip_i].y < lm[pip_i].y

    def _tip_below_pip(self, lm, tip_i, pip_i) -> bool:
        """Finger pointing DOWN: tip is lower than PIP (larger y)."""
        return lm[tip_i].y > lm[pip_i].y

    def _finger_down_extended(self, lm, tip_i, pip_i) -> bool:
        """
        Finger is genuinely extended downward:
          - tip is below PIP
          - tip is below wrist (whole finger hangs down)
          - tip is further from wrist than PIP (not just bent slightly)
        """
        return (
            lm[tip_i].y > lm[pip_i].y
            and lm[tip_i].y > lm[self.WRIST].y
            and self._dist(lm[tip_i], lm[self.WRIST])
                > self._dist(lm[pip_i], lm[self.WRIST])
        )

    def _thumb_horizontal(self, lm) -> bool:
        """
        Thumb is lying roughly horizontal:
          - Its tip is at roughly the same height as the wrist
          - It is extended outward (not tucked in)
        """
        y_diff = abs(lm[self.THUMB_TIP].y - lm[self.WRIST].y)
        dist   = self._dist(lm[self.THUMB_TIP], lm[self.WRIST])
        return y_diff < self.THUMB_H_TOL and dist > self.THUMB_EXTEND_MIN

    def _finger_folded(self, lm, tip_i, pip_i) -> bool:
        """Finger is folded/closed: tip below its own PIP joint."""
        return lm[tip_i].y > lm[pip_i].y

    # ── main classifier ─────────────────────────

    def classify(self, hand_landmarks, handedness_label: str) -> str:
        """
        Parameters
        ----------
        hand_landmarks    : MediaPipe hand landmark object
        handedness_label  : "Left" or "Right" as reported by MediaPipe
                            (remember: after flip, "Right" = user's left hand)

        Returns
        -------
        str : one of 'low_sign', 'medium_sign', 'high_sign', 'unknown'
        """
        lm = hand_landmarks.landmark

        # ── Pre-compute all finger states ─────────
        index_up    = self._tip_above_pip(lm, self.INDEX_TIP,  self.INDEX_PIP)
        middle_up   = self._tip_above_pip(lm, self.MIDDLE_TIP, self.MIDDLE_PIP)
        ring_up     = self._tip_above_pip(lm, self.RING_TIP,   self.RING_PIP)
        pinky_up    = self._tip_above_pip(lm, self.PINKY_TIP,  self.PINKY_PIP)

        middle_fold = self._finger_folded(lm, self.MIDDLE_TIP, self.MIDDLE_PIP)
        ring_fold   = self._finger_folded(lm, self.RING_TIP,   self.RING_PIP)
        pinky_fold  = self._finger_folded(lm, self.PINKY_TIP,  self.PINKY_PIP)

        index_down  = self._finger_down_extended(lm, self.INDEX_TIP,  self.INDEX_PIP)
        middle_down = self._finger_down_extended(lm, self.MIDDLE_TIP, self.MIDDLE_PIP)
        ring_down   = self._finger_down_extended(lm, self.RING_TIP,   self.RING_PIP)

        thumb_horiz = self._thumb_horizontal(lm)

        # ────────────────────────────────────────────────────────
        # LOW  –  LEFT hand only
        #   ✔ index pointing up
        #   ✔ thumb horizontal
        #   ✔ middle, ring, pinky all folded
        #   ✔ MediaPipe label "Right"  (= user's actual left after flip)
        # ────────────────────────────────────────────────────────
        if (handedness_label == "Right"    # user's left hand after mirror-flip
                and index_up
                and thumb_horiz
                and middle_fold
                and ring_fold
                and pinky_fold):
            return "low_sign"

        # ────────────────────────────────────────────────────────
        # MEDIUM  –  Either hand
        #   ✔ index, middle, ring all pointing DOWN (like 'M' shape)
        #   (thumb and pinky position not constrained)
        # ────────────────────────────────────────────────────────
        if index_down and middle_down and ring_down:
            return "medium_sign"

        # ────────────────────────────────────────────────────────
        # HIGH  –  Either hand  (Thai love / ILY sign)
        #   ✔ index pointing up
        #   ✔ pinky pointing up
        #   ✔ middle and ring folded
        #   ✔ thumb horizontal
        # ────────────────────────────────────────────────────────
        if (index_up
                and pinky_up
                and middle_fold
                and ring_fold
                and thumb_horiz):
            return "high_sign"

        return "unknown"


# ──────────────────────────────────────────────
# Event builder
# ──────────────────────────────────────────────
def build_event(camera_id: int, gesture: str) -> dict:
    meta = GESTURE_MAP.get(gesture, GESTURE_MAP["unknown"])
    return {
        "camera_id":    camera_id,
        "gesture":      gesture,
        "danger_level": meta["danger_level"],
        "priority":     meta["priority"],
        "timestamp":    datetime.now().isoformat(),
    }


# ──────────────────────────────────────────────
# Overlay helpers
# ──────────────────────────────────────────────
def _draw_progress_bar(frame, x, y, w, h, ratio, bg_color, fg_color, label):
    cv2.rectangle(frame, (x, y), (x + w, y + h), bg_color, -1)
    filled = int(w * min(ratio, 1.0))
    if filled > 0:
        cv2.rectangle(frame, (x, y), (x + filled, y + h), fg_color, -1)
    cv2.rectangle(frame, (x, y), (x + w, y + h), (255, 255, 255), 1)
    cv2.putText(frame, label, (x, y - 6),
                cv2.FONT_HERSHEY_SIMPLEX, 0.50, (255, 255, 255), 1)


def _draw_key_legend(frame):
    """
    Semi-transparent legend in the bottom-left corner showing all gestures
    and the [Q] quit key.
    """
    fh, fw = frame.shape[:2]
    lines = [
        ("-- GESTURES --",              (200, 200, 200)),
        ("LOW  : L-hand index up",      (0,   200,   0)),
        ("       + thumb horizontal",   (0,   180,   0)),
        ("MED  : 3 fingers down (M)",   (0,   165, 255)),
        ("HIGH : ILY sign (Thai love)", (0,     0, 255)),
        ("",                            (0, 0, 0)),
        ("[Q]  Quit all cameras",        (220, 220,  50)),
    ]
    box_x  = 8
    box_h  = len(lines) * 18 + 10
    box_y  = fh - box_h - 8
    box_w  = 250

    overlay = frame.copy()
    cv2.rectangle(overlay, (box_x, box_y),
                  (box_x + box_w, box_y + box_h), (15, 15, 15), -1)
    cv2.addWeighted(overlay, 0.60, frame, 0.40, 0, frame)

    for i, (text, color) in enumerate(lines):
        if text:
            cv2.putText(frame, text, (box_x + 6, box_y + 16 + i * 18),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.40, color, 1, cv2.LINE_AA)


def _draw_overlay(frame, event, phase, phase_ratio, cam_label):
    """
    Draw the top banner, progress bar, timestamp, and key legend.

    phase       : 'confirming' | 'cooldown' | 'idle'
    phase_ratio : 0.0 → 1.0 progress through the current phase
    """
    fh, fw = frame.shape[:2]
    color  = LEVEL_COLORS.get(event["danger_level"], (80, 80, 80))

    # ── top banner ──────────────────────────────
    cv2.rectangle(frame, (0, 0), (fw, 55), color, -1)
    g_label = f"{event['gesture'].upper()}  |  {event['danger_level']}"
    cv2.putText(frame, g_label, (10, 38),
                cv2.FONT_HERSHEY_SIMPLEX, 0.80, (255, 255, 255), 2)
    cv2.putText(frame, cam_label, (fw - 120, 38),
                cv2.FONT_HERSHEY_SIMPLEX, 0.65, (255, 255, 255), 2)

    # ── progress bar ────────────────────────────
    bx, by, bw, bh = 10, fh - 38, fw - 20, 16

    if phase == "confirming":
        secs = CONFIRM_SECONDS * (1.0 - phase_ratio)
        _draw_progress_bar(frame, bx, by, bw, bh,
                           phase_ratio, (50, 50, 50), color,
                           f"Hold to confirm...  {secs:.1f}s")

    elif phase == "cooldown":
        secs = COOLDOWN_SECONDS * (1.0 - phase_ratio)
        _draw_progress_bar(frame, bx, by, bw, bh,
                           phase_ratio, (50, 50, 50), (110, 110, 110),
                           f"Cooldown...  {secs:.1f}s")
        # dim banner
        ov = frame.copy()
        cv2.rectangle(ov, (0, 0), (fw, 55), (0, 0, 0), -1)
        cv2.addWeighted(ov, 0.45, frame, 0.55, 0, frame)
        cv2.putText(frame, "COOLDOWN", (fw // 2 - 65, 38),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.85, (160, 160, 160), 2)

    # ── timestamp ───────────────────────────────
    ts = event["timestamp"][11:19]
    cv2.putText(frame, ts, (fw - 85, fh - 6),
                cv2.FONT_HERSHEY_SIMPLEX, 0.50, (160, 160, 160), 1)

    # ── key legend ──────────────────────────────
    _draw_key_legend(frame)


# ──────────────────────────────────────────────
# Per-camera worker thread
# ──────────────────────────────────────────────
class CameraWorker(threading.Thread):
    """
    One thread per camera.  State machine: idle → confirming → cooldown → idle.
    Fires on_event(dict) callback when a gesture is confirmed.
    """

    def __init__(self, camera_id: int, device_index: int, on_event,
                 confirm_secs: float = CONFIRM_SECONDS,
                 cooldown_secs: float = COOLDOWN_SECONDS):
        super().__init__(daemon=True)
        self.camera_id     = camera_id
        self.device_index  = device_index
        self.on_event      = on_event
        self.confirm_secs  = confirm_secs
        self.cooldown_secs = cooldown_secs
        self.stop_flag     = threading.Event()

        self.classifier        = GestureClassifier()
        self.mp_hands          = mp_hands_module
        self.mp_drawing        = mp_drawing_module
        self.mp_drawing_styles = mp_drawing_styles_module

    def run(self):
        hands = self.mp_hands.Hands(
            static_image_mode=False,
            max_num_hands=1,
            min_detection_confidence=0.75,
            min_tracking_confidence=0.60,
        )

        cap = cv2.VideoCapture(self.device_index)
        if not cap.isOpened():
            print(f"[Camera {self.camera_id}] ERROR: cannot open "
                  f"device {self.device_index}")
            return

        cam_label = f"CAM {self.camera_id}"
        window    = f"CCTV – Camera {self.camera_id}"
        print(f"[Camera {self.camera_id}] Started  (device {self.device_index})")

        # state
        current_gesture = None
        gesture_start   = None
        phase           = "idle"
        cooldown_start  = None
        last_event      = build_event(self.camera_id, "unknown")

        try:
            while not self.stop_flag.is_set():
                ret, frame = cap.read()
                if not ret:
                    continue

                frame   = cv2.flip(frame, 1)           # mirror
                rgb     = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                results = hands.process(rgb)

                now = time()

                # ── gesture detection ────────────
                detected       = None
                handedness_lbl = None

                if results.multi_hand_landmarks and results.multi_handedness:
                    hand_lm        = results.multi_hand_landmarks[0]
                    handedness_lbl = (results.multi_handedness[0]
                                      .classification[0].label)

                    # draw skeleton
                    self.mp_drawing.draw_landmarks(
                        frame, hand_lm,
                        self.mp_hands.HAND_CONNECTIONS,
                        self.mp_drawing_styles.get_default_hand_landmarks_style(),
                        self.mp_drawing_styles.get_default_hand_connections_style(),
                    )

                    # small handedness label near wrist
                    wrist_px = hand_lm.landmark[0]
                    fh, fw   = frame.shape[:2]
                    wx = int(wrist_px.x * fw)
                    wy = int(wrist_px.y * fh)
                    user_side = "L-hand" if handedness_lbl == "Right" else "R-hand"
                    cv2.putText(frame, user_side, (wx - 20, wy + 20),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                                (255, 220, 0), 1, cv2.LINE_AA)

                    detected = self.classifier.classify(hand_lm, handedness_lbl)

                # ── state machine ────────────────

                if phase == "cooldown":
                    elapsed = now - cooldown_start
                    ratio   = elapsed / self.cooldown_secs
                    if elapsed >= self.cooldown_secs:
                        phase           = "idle"
                        current_gesture = None
                        gesture_start   = None
                    _draw_overlay(frame, last_event, "cooldown", ratio, cam_label)

                elif phase == "confirming":
                    if detected != current_gesture:
                        # gesture broke – reset
                        current_gesture = detected
                        gesture_start   = now if (detected and detected != "unknown") else None
                        phase = "confirming" if gesture_start else "idle"
                    else:
                        elapsed = now - gesture_start
                        ratio   = elapsed / self.confirm_secs
                        _draw_overlay(frame, last_event, "confirming",
                                      ratio, cam_label)
                        if elapsed >= self.confirm_secs:
                            # ✅ confirmed
                            event      = build_event(self.camera_id, current_gesture)
                            last_event = event
                            self.on_event(event)
                            phase           = "cooldown"
                            cooldown_start  = now
                            current_gesture = None

                else:  # idle
                    if detected and detected != "unknown":
                        if detected != current_gesture:
                            current_gesture = detected
                            gesture_start   = now
                        else:
                            elapsed = now - gesture_start
                            ratio   = elapsed / self.confirm_secs
                            last_event = build_event(self.camera_id, detected)
                            _draw_overlay(frame, last_event, "confirming",
                                          ratio, cam_label)
                            if ratio >= 0.01:          # tiny delay before switching phase
                                phase = "confirming"
                    else:
                        current_gesture = None
                        gesture_start   = None
                        fh2, fw2 = frame.shape[:2]
                        cv2.putText(frame, f"{cam_label} – Show your hand",
                                    (10, 38), cv2.FONT_HERSHEY_SIMPLEX,
                                    0.8, (110, 110, 110), 2)
                        _draw_key_legend(frame)

                cv2.imshow(window, frame)

                if cv2.waitKey(1) & 0xFF == ord("q"):
                    print(f"\n[Camera {self.camera_id}] 'Q' pressed – stopping all cameras.")
                    self.stop_flag.set()
                    break

        finally:
            cap.release()
            hands.close()
            cv2.destroyWindow(window)
            print(f"[Camera {self.camera_id}] Stopped.")


# ──────────────────────────────────────────────
# Multi-camera launcher
# ──────────────────────────────────────────────
class MultiCameraSystem:
    """
    Launches one CameraWorker thread per (camera_id, device_index) pair.
    All workers share the same on_event callback.
    """

    def __init__(self, cameras: list[tuple[int, int]], on_event):
        self.workers = [
            CameraWorker(cam_id, dev_idx, on_event)
            for cam_id, dev_idx in cameras
        ]

    def start(self):
        for w in self.workers:
            w.start()

    def wait(self):
        for w in self.workers:
            w.join()

    def stop_all(self):
        for w in self.workers:
            w.stop_flag.set()
