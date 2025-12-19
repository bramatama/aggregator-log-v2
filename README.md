# Aggregator Multiservice - UAS Sistem Paralel dan Terdistribusi

Proyek ini adalah implementasi sistem **Event Aggregator** yang dirancang untuk menangani *ingestion* data bervolume tinggi secara asinkron dan terdistribusi. Sistem ini menggunakan pola arsitektur *Producer-Consumer* dengan **Redis** sebagai message broker dan **PostgreSQL** sebagai penyimpanan persisten.

## 🚀 Cara Menjalankan

Sistem ini dikemas sepenuhnya menggunakan Docker. Pastikan Docker Desktop atau Docker Engine sudah terinstall.

1. **Jalankan semua service utama:**
   ```bash
   docker compose up --build
   ```
   Perintah ini akan menjalankan service `aggregator`, `broker`, `storage`, dan `publisher` (load tester).

2. **Hentikan sistem:**
   Tekan `Ctrl+C` atau jalankan:
   ```bash
   docker compose down
   ```

---

## 🐳 Highlight: Docker Compose & Arsitektur

Dalam konteks Sistem Terdistribusi, `docker-compose.yml` bertindak sebagai orkestrator yang menghubungkan berbagai komponen independen dalam jaringan internal (`backend-local-network`).

Service yang berjalan:
1.  **Aggregator (`aggregator:8080`)**:
    *   Service utama berbasis Python (FastAPI).
    *   Berfungsi menerima HTTP Request dan langsung mengembalikan respons `202 Accepted` (Non-blocking I/O).
    *   Mengirim tugas ke antrian Redis untuk diproses oleh *background worker*.
2.  **Broker (`redis:6379`)**:
    *   Bertindak sebagai *Message Queue* sementara untuk menampung lonjakan trafik (*traffic spike*) agar database tidak kewalahan.
3.  **Storage (`postgres:5432`)**:
    *   Menyimpan data event yang sudah diproses dan divalidasi.
4.  **Publisher**:
    *   Service simulasi yang mengirim ribuan event secara konkuren untuk menguji konkurensi dan mekanisme deduplikasi sistem.

---

## 📜 Highlight: Monitoring Aggregator Log

Karena sistem ini bersifat **Asynchronous**, respons HTTP sukses tidak berarti data sudah masuk database, melainkan baru masuk antrian. Oleh karena itu, **Log Aggregator** sangat krusial untuk memverifikasi pemrosesan paralel yang sebenarnya.

Untuk melihat proses yang terjadi di belakang layar (worker), gunakan perintah:

```bash
docker compose logs -f aggregator
```

**Apa yang harus diperhatikan di log?**
*   `Enqueued event`: Menandakan API berhasil menerima request dan menaruhnya di Redis.
*   `Processing event`: Menandakan Worker berhasil mengambil event dari Redis.
*   `Duplicate dropped`: Menandakan logika deduplikasi berhasil mendeteksi event ganda (penting untuk *Idempotency*).
*   `Saved to DB`: Menandakan data final berhasil disimpan ke PostgreSQL.

---

## 🧪 Menjalankan Test Case (Integration Test)

Terdapat container khusus `test_case` yang berisi skenario pengujian otomatis menggunakan `pytest`. Container ini berada dalam profil `testing` agar tidak mengganggu aliran utama saat `docker compose up` biasa.

Untuk menjalankan pengujian:

```bash
docker compose --profile testing up test_case
```

Atau menjalankannya secara interaktif:
```bash
docker compose run --rm test_case pytest -v
```
