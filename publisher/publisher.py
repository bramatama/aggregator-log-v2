import os
import time
import uuid
import random
import requests
import datetime
import sys
import threading

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
DELAY = float(os.getenv("DELAY", "0"))
DUPLICATION_RATE = float(os.getenv("DUPLICATION_RATE", "0.3")) 
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "20000"))

# Derived Config
if "/publish" in TARGET_URL:
    STATS_URL = TARGET_URL.replace("/publish", "/stats")
else:
    STATS_URL = TARGET_URL.rsplit("/", 1)[0] + "/stats"

responsive_check_passed = None

def generate_event(topic, event_id):
    return {
        "topic": topic,
        "event_id": str(event_id),
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "publisher-service",
        "payload": {
            "amount": random.randint(10, 1000),
            "user_id": random.randint(1, 500),
            "run_id": str(uuid.uuid4())
        }
    }

def check_responsiveness():
    """Thread terpisah yang mengecek /stats saat load test berjalan."""
    global responsive_check_passed
    try:
        start_time = time.time()
        res = requests.get(STATS_URL, timeout=2.0)
        end_time = time.time()
        
        if res.status_code == 200:
            print(f"[Responsiveness Check] BERHASIL! /stats merespons dalam {end_time - start_time:.2f} detik.")
            responsive_check_passed = True
        else:
            print(f"[Responsiveness Check] GAGAL! /stats mengembalikan {res.status_code}")
            responsive_check_passed = False
    except Exception as e:
        print(f"[Responsiveness Check] GAGAL! Error: {e}")
        responsive_check_passed = False

def run_publisher():    
    # --- Persiapan Data (Pre-generation) ---
    num_unique = int(MAX_EVENTS * (1 - DUPLICATION_RATE))
    num_duplicates = MAX_EVENTS - num_unique
    print(f"Rencana: {num_unique} Unik, {num_duplicates} Duplikat")

    unique_events = []
    for _ in range(num_unique):
        topic = random.choice(["order.created", "payment.success", "user.login", "sensor.read"])
        unique_events.append(generate_event(topic, uuid.uuid4()))
    
    duplicate_events = [random.choice(unique_events) for _ in range(num_duplicates)]
    all_events_to_send = unique_events + duplicate_events
    random.shuffle(all_events_to_send)
    
    print(f"Total event disiapkan: {len(all_events_to_send)}")

    time.sleep(5)
    
    # --- Cek Stats Awal ---
    try:
        stats_before = requests.get(STATS_URL).json().get("uptime_stats", {})
        print(f"Stats Awal -> Unik: {stats_before.get('unique_processed', 0)}, Dropped: {stats_before.get('duplicate_dropped', 0)}")
    except Exception:
        stats_before = {"unique_processed": 0, "duplicate_dropped": 0}

    # --- Mulai Pengiriman ---
    start_time = time.time()
    session = requests.Session()
    
    # Jadwalkan cek responsivitas
    threading.Timer(5.0, check_responsiveness).start()
    threading.Timer(15.0, check_responsiveness).start()

    count = 0
    for event in all_events_to_send:
        try:
            response = session.post(TARGET_URL, json=event, timeout=5)
            count += 1
            if count % 500 == 0:
                print(f"Progress: {count}/{MAX_EVENTS} events sent... (Last: {response.status_code})")
            
            if DELAY > 0:
                time.sleep(DELAY)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nSELESAI! Terkirim: {count} events.")
    print(f"⏱️ Waktu Total: {duration:.2f} detik")
    
    print("Menunggu processing di aggregator (5s)...")
    time.sleep(5)

    # --- Validasi Akhir ---
    try:
        stats_after = requests.get(STATS_URL).json().get("uptime_stats", {})
        
        delta_unique = stats_after.get('unique_processed', 0) - stats_before.get('unique_processed', 0)
        delta_dropped = stats_after.get('duplicate_dropped', 0) - stats_before.get('duplicate_dropped', 0)

        print("\n--- Hasil Validasi ---")
        print(f"Total Unik Diproses: {delta_unique} (Target: {num_unique})")
        print(f"Total Duplikat Dibuang: {delta_dropped} (Target: {num_duplicates})")

        if abs(delta_unique - num_unique) <= 10:
            print("Validasi Unik: OK")
        else:
            print("Validasi Unik: MISMATCH")

        if responsive_check_passed:
            print("Responsiveness: OK")
        else:
            print("Responsiveness: Check Failed/Not Run")
            
    except Exception as e:
        print(f"Gagal validasi akhir: {e}")
    
    print("Publisher idle (menunggu dimatikan manual)...")
    while True:
        time.sleep(5)

if __name__ == "__main__":
    run_publisher()