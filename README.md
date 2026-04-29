# EKK Site Poller

Bu klasör, saha bazlı EKK cihazı için PostgreSQL tablolarını ve Python polling job'unu içerir.

## Proje yapısı

- `main.py`: referans projedeki gibi ince giriş noktası
- `utils/db.py`: PostgreSQL bağlantısı ve log insert yardımcıları
- `utils/logger.py`: dosya tabanlı hata logger'ı
- `utils/modbus_utils.py`: EKK cihazını bulma, Modbus okuma, DB'ye yazma
- `sql/`: migration ve seed dosyaları

## Oluşturulan tablolar

- `public.ekk_device`
- `public.ekk_device_register_map`
- `public.ekk_device_reading`
- `public.ekk_device_reading_value`
- `public.ekk_device_poll_log`

`ekk_device` tablosuna operasyonel olarak gerekli olduğu için `modbus_host`, `modbus_port`, `modbus_unit_id` ve `web_base_url` alanları eklendi. `ekk_device_register_map` tablosuna ise PDF'deki scaling bilgisini kaybetmemek için `scale_multiplier` alanı eklendi.

## Kurulum

1. [001_create_ekk_tables.sql](/D:/OneDrive - BAKIRÇAY ÜNİVERSİTESİ/DEVELOPER/sysEnerji/_2026_PVSENSE/codex/EKK/sql/001_create_ekk_tables.sql) dosyasını çalıştırın.
2. [002_seed_sys_cati_ion7650.sql](/D:/OneDrive - BAKIRÇAY ÜNİVERSİTESİ/DEVELOPER/sysEnerji/_2026_PVSENSE/codex/EKK/sql/002_seed_sys_cati_ion7650.sql) dosyasını çalıştırın.
3. Python bağımlılıklarını kurun:

```bash
pip install -r requirements.txt
```

4. Ortam değişkenlerini tanımlayın:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
export LOG_LEVEL=INFO
export EKK_INTERVAL_SECONDS=300
```

## Çalıştırma

Tek seferlik test:

```bash
python3 main.py 11
```

PM2 ile:

```bash
pm2 start main.py --interpreter python3 --name ekk_job_site_11 -- 11
```

## Notlar

- Seed dosyası `power_plant.id = 82` kaydının `site_id` bilgisini kullanır.
- Modbus unit id değeri verilen bilgilerde olmadığı için varsayılan olarak `1` kullanıldı.
- PDF içinde web ekranındaki peak-demand zaman damgaları için ayrı register adresleri net ayıklanamadı. Bu nedenle ilk seed, operasyon, tüketim ve power quality ekranlarında görünen ana anlık/enerji/harmonik alanlarını kapsar.
- Yapı, verdiğiniz `python_modbus` örneğine benzer olacak şekilde `main.py + utils/` formuna çevrildi.
