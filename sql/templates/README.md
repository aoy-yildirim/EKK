# Yeni EKK ve register haritası ekleme

Bu dosyalar elle çalıştırılan şablonlardır; otomatik migration değildir.
Mevcut tablo yapısını kullanırlar. Yeni bir tip/profil tablosu oluşturmazlar.

CET iMeter 7A için `insert_imeter7a.sql` ve modele özgü adresleme,
bağlantı, kapsam ve doğrulama notları için [IMETER7A.md](IMETER7A.md)
dosyasını kullanın. Aşağıdaki 38 satırlık kapsam ION modellerine aittir.

## İlk ION7400 veya ION9000

1. İlgili `insert_ion7400.sql` veya `insert_ion9000.sql` dosyasını açın.
2. `DECLARE` bölümünde `p_site_id`, `p_serial_number`, `p_name`,
   `p_modbus_host`, `p_modbus_port`, `p_modbus_unit_id`, `p_web_base_url`
   değerlerini doldurun. `site_id`, `power_plant_id` değildir.
3. Modeli ve firmware uyumluluğunu doğrulayın:
   ION7400: 001.002.14; ION9000: 004.005.000.
4. PostgreSQL üzerinde tüm dosyayı tek parça çalıştırın.
5. Oluşturulan `ekk_device_id`, PostgreSQL NOTICE mesajında gösterilir.

`id` elle verilmez; veritabanı yeni cihaz ve harita ID'lerini üretir.
Eksik zorunlu parametrede işlem durur. Bir hata tüm DO bloğunu geri alır.
Mevcut site/seri numarasıyla tekrar çalıştırma mevcut kaydı güncellemez;
proje şemasındaki UNIQUE kısıtları nedeniyle hata verir.
Proje şeması saha başına bir cihaz kabul eder (`UNIQUE(site_id)`).

## Sonraki aynı model cihaz

`clone_ekk_device.sql` dosyasında `p_source_device_id` alanına doğrulanmış
örnek cihazı, diğer parametrelere yeni sahanın bilgilerini yazıp çalıştırın.
Marka/model ve tüm register satırları kaynaktan kopyalanır. Yeni ID'ler,
UUID ve oluşturma zamanları otomatik üretilir. Kaynağın IP, port, seri
numarası ve okuma geçmişi kopyalanmaz. Kaynak harita ID'leri korunur.
Kapalı register satırları kapalı olarak kopyalanır.

Aynı modelde firmware, özel harita, CT/PT ve enerji yönü farklılıklarını
kontrol edin. Kaynak cihazın saha özelindeki harita değişiklikleri de kopyalanır.

## Kapsam ve okumaya geçiş

Her model şablonu 38 register tanımı ekler: 32 etkin, 6 kapalı.
Temel akım, gerilim, güç, frekans ve birikimli enerji ölçümleri dahildir.
Güçler W/VAR/VA'dan kW/kVAR/kVA'ya; INT64 enerjiler Wh/VARh/VAh'dan
kWh/kVARh/kVAh'ya 0.001 çarpanıyla dönüştürülür.
`modbus_units`, çarpan uygulandıktan sonraki hedef birimdir.

PF (4 alan) ve dengesizlik (2 alan), kaynak ölçeği ve işaret kuralı
doğrulanana kadar `is_enabled=false` kalır. Bu satırlardaki 1.0 çarpanı
geçicidir. Mevcut PF standardı yüzdedir (örneğin 99.99).
THD, K/crest factor ve maksimum/demand alanları bu ilk şablonlara dahil
edilmemiştir. Özellikle 3 saniyelik THD ile eski haritadaki THD max aynı
ölçüm kabul edilmemelidir. Eksik alanlar sıfır olarak üretilmez.

Tüm şablonlar cihazı **status=0** ile oluşturur. Güncellenmiş Python okuyucu,
`ekk_device.model` alanına göre adres dönüşümü yapar: ION7650 için 40001,
ION7400 ve ION9000 için 1 çıkarır. Model alanında `PowerLogic ION7400`,
`PowerLogic ION9000` veya `PowerLogic ION7650` gibi model adını kullanın.
Bilinmeyen modeller tahmin edilmez; açık hata ile durdurulur. Bu yeni
haritalar kaynak Excel'deki yerel register numaralarını saklar; adresleri
veritabanında 4xxxx biçimine çevirmeyin.
INT64 desteği eklenmiştir. Enerji ölçeklemesinde Decimal hassasiyeti korunur.
`reading_value.value_numeric` PostgreSQL NUMERIC olarak kalır; INT64 kaynaklı
değerler JSON `raw_payload` içinde hassasiyeti korumak için ondalık metindir.
Diğer mevcut sayısal ölçümlerin JSON biçimi değişmez.

Güncel okuyucu ile adres başlangıcı, word/byte sırası, CT/PT
ölçeklemesi ve enerji yönünü cihaz ekranıyla tek okuma üzerinden doğrulayın.
Ardından yalnızca ilgili cihazı etkinleştirin. Kopyalanan bir cihaz için de
bu kontrol gerekir. Bu şablonların hazırlanması sırasında canlı veritabanına
veya saha cihazlarına işlem yapılmamıştır.

## Kaynaklar ve kontrol

- ION7400_Modbus_Map.xlsx: Detailed Modbus Map; readme FW 001.002.14.
- ION9000_Modbus_Map_004.005.000.xlsx: Detailed Modbus Map; FW 004.005.000.

İki şablondaki 38 adresin tamamı, register uzunlukları ve veri tipleri
ilgili Excel ile karşılaştırıldı. Quantity ve adres tekrarları kontrol edildi.
PostgreSQL üzerinde çalıştırma ve canlı cihaz doğrulaması yapılmadı.
