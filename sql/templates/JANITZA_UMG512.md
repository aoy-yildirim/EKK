# Janitza UMG512 entegrasyonu

Mevcut `ekk_device` ve `ekk_device_register_map` üzerinden çalışır.
Şema değişikliği yoktur. Okuyucudaki ekleme yalnızca UMG512 modelinin
adres seçimine aittir; diğer modellerin adresleme/çözümleme dalları değişmez.

## Kaynak ve uyumluluk

- Verilen kaynak: `janitza-mal-umg512-en.pdf`, Dok. Nr. 1.054.016.2.d.
- Sayfa 5: listelenen adresler Big Endian; sayısal tipler.
- Sayfa 12, 14-15: anlık ölçümler, 200ms pencere/güncelleme.
- Sayfa 42-43: enerji sayaçları, tüm tarifeler ve tarife özelindeki sayaçlar.
- Üretici adres örneği: https://www.janitza.com/files/download/manuals/current/UMG512/janitza-bhb-umg512ul-en.pdf
  Sayfa 90: U L1-N için FC03, PDU başlangıç adresi 0x4A38 = 19000,
  uzunluk 2. Adresten 1 veya 40001 çıkarılmaz.

Ekranda model UMG512, firmware Rel.5.029, seri 4203.4521 görünüyor.
Bu haritanın tüm satırlarının sahadaki firmware ile canlı uyumu henüz
doğrulanmadı. Web arayüzü alt bilgisindeki UMG512-PRO yazısı tek başına
model değişikliği kanıtı sayılmaz; esas cihaz modeli/etiketi kontrol edilir.
Başka UMG modelleri veya UMG512-PRO otomatik olarak bu profile bağlanmaz.

## Hazırlanan kapsam

`insert_janitza_umg512.sql`: 49 tanım, 42 etkin ve 7 kapalı PF/cos-phi alanı.
Bütün satırlar 2 register uzunluğunda FLOAT32 ve Big Endian'dır.

| Ölçüm grubu | Adet | Durum |
|---|---:|---|
| Akım L1/L2/L3/L4 | 4 | Etkin |
| Gerilim L-N ve L-L | 6 | Etkin |
| Frekans | 1 | Etkin |
| Faz ve toplam P/Q/S | 12 | Etkin |
| Aktif import/export, görünür toplam enerji | 3 | Etkin |
| Net aktif/reaktif, endüktif/kapasitif enerji | 4 | Etkin, ayrı quantity |
| Anlık THD (200ms) | 6 | Etkin, ayrı quantity |
| K factor ve crest factor | 6 | Etkin |
| Faz true PF | 3 | Kapalı, işaret doğrulanmalı |
| Faz/toplam fundamental cos-phi | 4 | Kapalı, ayrı quantity |

## Eşleştirme ve önemli ayrımlar

- `kw_total` 19026, `kva_total` 19034 ve `kvar_total` 19042 L1..L3'ü kapsar.
  L4 ayrı tutulur; `i4` 3859 gerçek L4 akımıdır. 19018 vektörel akım
  toplamıdır ve `i_avg` ya da `i4` yerine kullanılmaz.
- Akımlar A, gerilimler V, frekans Hz olarak saklanır. P/Q/S, W/var/VA'dan
  0.001 ile kW/kVAR/kVA'ya dönüştürülür. Ekranın kW/kV gösterimi ikinci
  bir CT/PT çarpanı uygulama gerekçesi değildir.
- Kılavuz Q alanlarını temel bileşenin reaktif gücü olarak tanımlar.
  `kvar_*` satırlarının notes alanı bunu belirtir. Farklı üreticilerdeki
  harmonik içeren reaktif güç hesaplarıyla birebir eşdeğer varsayılmamalıdır.
- Schneider'deki delivered/received alan adları yük perspektifiyle korunur:
  `kwh_del` = Janitza consumed `_WH_V[4]` (6508), `kwh_rec` = Janitza
  delivered `_WH_Z[4]` (6520). Janitza'nın delivered kelimesi doğrudan
  mevcut `kwh_del` alanına kopyalanmaz. Sahadaki CT yönü yine doğrulanır.
- 19068 yalnızca tüketim tarife 1 sayacıdır. Tüm tarifeleri kapsayan
  import için 6508 seçilmiştir.
- Ekrandaki negatif toplam kWh, import/export yerine `kwh_net` 6484 ile
  karşılaştırılır. `kvah_del_rec` 6472 görünür toplam enerjidir.
- `kvarh_inductive` 6580 ve `kvarh_capacitive` 6592, reaktif import/export
  değildir. `kvarh_del` ve `kvarh_rec` için sahte eşleştirme yapılmaz.
  `kvarh_net` 6496 ayrı saklanır.
- Enerji kaynağı FLOAT32 Wh/varh/VAh'dır; INT64 değildir. 0.001 ile
  kWh/kVARh/kVAh'ya dönüştürülür. Büyük birikimli sayaçlarda FLOAT32'nin
  çözünürlük sınırı vardır: örneğin yaklaşık 35 GWh'ta birkaç kWh adım
  oluşabilir. NUMERIC'e yazmak kaynak hassasiyetini artırmaz. Çok kısa
  aralıklardaki sayaç farkları bu sınır gözetilerek değerlendirilmelidir.
- `pf_sign_a/b/c` true PF'dir (3893/3895/3897), oranı yüzdeye çevirmek
  için çarpan 100'dür. `cos_phi_*` temel bileşen cos-phi'sidir ve oran
  olarak saklanır. Ekrandaki cos-phi ile true PF birbirine eşit sayılmaz.
  Kılavuzda doğrulanmış toplam true PF registerı seçilmediği için
  `pf_sign_total` uydurulmamıştır. Bu 7 alan ilk şablonda kapalıdır.
- `v1_thd_200ms` vb. adlar anlık 200ms THD'yi ifade eder; mevcut THD max
  veya ION THD 3s alanlarının yerine yazılmaz. Kaynak birim % ve çarpan 1'dir.
- `i_avg`, `vln_avg`, `vll_avg` kaynakta doğrulanmış faz ortalaması adresi
  bulunmadığından eklenmez. Kılavuzun "mean value" adresleri zaman
  ortalamasıdır ve faz ortalaması yerine kullanılamaz.
- Demand, min/max ve dengesizlik ilk kapsamda yoktur. Bulunmayan alanlar
  sıfıra çevrilmemeli; sorgu/arayüz eksik ölçüm olarak ele almalıdır.

## Yeni sahaya ekleme

1. `insert_janitza_umg512.sql` DECLARE bölümünde site_id, seri numarası,
   ad, erişilebilir IP, port, unit ID ve isteğe bağlı web URL'sini doldurun.
   Desteklenen model adları: `UMG512`, `UMG 512`, `Janitza UMG512`.
2. Ekrandaki yerel IP `10.125.133.7`, gateway `10.125.133.1`.
   WAN erişiminde dış IP/dış port girilir; dış port 40502 varsayılmaz.
   Janitza'nın Modbus TCP servisi 502 portunu kullanır. NAT varsa hedef
   cihazın yerel IP'si:502 olmalıdır.
3. Ekrandaki adres 1, RS485 bölümündedir. Şablonda 1 başlangıç değeri
   kullanılır; TCP/gateway unit ID ayrıca doğrulanmalıdır.
4. Scripti PostgreSQL üzerinde çalıştırın. ID otomatik üretilir ve NOTICE
   olarak gösterilir. Aynı site/seri ile tekrar çalıştırma mevcut kaydı
   güncellemez; UNIQUE ihlaliyle tüm blok geri alınır.
5. Yeni cihaz `status=0` oluşturulur. Cihaz ayarları doğrulanıp ilgili
   cihaz etkinleştirildikten sonra `python main.py SITE_ID --once` ile
   tek okuma alın ve aynı zamandaki web ekranıyla karşılaştırın.
6. Aynı model yeni sahaya geldiğinde `clone_ekk_device.sql` kullanılabilir;
   kaynak haritadaki saha özelindeki ayarlar ve kapalı satırlar da kopyalanır.

## Doğrulama

Önceki ION/iMeter testleriyle birlikte model adreslemesi, Big Endian
FLOAT32, güç işareti, W->kW ve Wh->kWh dönüşümleri çevrimdışı sınanır.
Haritadaki 49 adres/tip kaynak PDF ile karşılaştırılır.
SQL canlı veritabanında çalıştırılmadı; cihaza bağlantı kurulmadı.
Hiçbir sayaç, cihaz ayarı veya geçmiş kayıt değiştirilmedi.
