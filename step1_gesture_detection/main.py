"""
main.py  –  Step 1 Entry Point (v3)
"""

from gesture_detector   import MultiCameraSystem
from priority_processor import AlertProcessor

CAMERA_SETUP = [
    (1, 0),   # Camera 1 → first webcam
    (2, 1),   # Camera 2 → second webcam  (remove this line if you only have one)
]

def main():
    print("=" * 62)
    print("  CCTV GESTURE ALERT SYSTEM  –  Step 1")
    print("=" * 62)
    print(f"  Launching {len(CAMERA_SETUP)} camera(s)...\n")
    print("  Gesture → Danger Level:")
    print("    LOW  (priority 1) : LEFT hand – index up + thumb horizontal")
    print("    MED  (priority 2) : Either hand – index/middle/ring DOWN (M)")
    print("    HIGH (priority 3) : Either hand – Thai love (ILY) sign")
    print("    (anything else)   : ignored / unknown\n")
    print("  Controls:")
    print("    [Q]  Quit all cameras\n")
    print(f"  Hold gesture for 3s to confirm.  3s cooldown after each event.")
    print("=" * 62 + "\n")

    processor = AlertProcessor(dedup_seconds=0)

    def handle_event(event):
        level = event["danger_level"]
        cam   = event["camera_id"]
        if event["priority"] < 3:
            print(f"  [CAM {cam}]  {level:<8}  "
                  f"gesture={event['gesture']:<14}  "
                  f"priority={event['priority']}  "
                  f"ts={event['timestamp'][11:19]}")
        processor.process(event)

    system = MultiCameraSystem(cameras=CAMERA_SETUP, on_event=handle_event)

    try:
        system.start()
        system.wait()
    except KeyboardInterrupt:
        print("\n[main] Interrupted.")
        system.stop_all()
    finally:
        processor.print_stats()
        print("[main] All cameras stopped.")

if __name__ == "__main__":
    main()
