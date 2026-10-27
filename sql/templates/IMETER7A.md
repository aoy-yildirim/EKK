# CET iMeter 7A entegrasyonu

Mevcut `ekk_device`, `ekk_device_register_map`, `ekk_device_reading` ve
`ekk_device_reading_value` tabloları kullanılır. Şema değişikliği yoktur.

## Hazırlanan dosyalar

- `insert_imeter7a.sql`: yeni cihaz ve temel RMS/enerji haritası oluşturur.
- `clone_ekk_device.sql`: doğrulanmış aynı model cihazı yeni sahaya kopyalar.
- `utils/modbus_utils.py`: `iMeter 7A`, `iMeter7A`, `CET iMeter 7A`
  model adlarını sıfır tabanlı adresleme ile destekler.

Kaynak: `iMeter 7A English User Manual V1.1 (20230221).pdf`, Modbus
Protocol Version 7.0; basılı s.134-136 (PDF s.142-144), s.138 (PDF s.146).
Big Endian FLOAT, kodda FLOAT32 olarak tanımlanır; enerji INT64'tür.
Okuma Function 03 ile yapılır. Cihaza yazma/sıfırlama komutu gönderilmez.
Register `0000` doğrudan PDU adresi 0'dır; ION modellerindeki adres çıkarma
işlemi bu modele uygulanmaz. Kaynak register numaraları ondalıktır.

## İlk kapsam

Toplam 35 tanım: 31 etkin ve 4 kapalı PF alanı (3P4W seçimiyle).
Akım (A/B/C/I4/ortalama), gerilim (faz-nötr ve faz-faz/ortalamalar),
faz/toplam P/Q/S, frekans ve 5 birikimli enerji sayacı dahildir.
Haritada `i5` için aynı anlamda bir temel ölçüm bulunmadığından eklenmez.

| Ortak alan | Adres | Kaynak | Kayıt dönüşümü |
|---|---:|---|---|
| vln_a | 0 | FLOAT, V | 1 -> V |
| i_a | 16 | FLOAT, A | 1 -> A |
| kw_total | 30 | FLOAT, W | 0.001 -> kW |
| kvar_total | 38 | FLOAT, var | 0.001 -> kVAR |
| kva_total | 46 | FLOAT, VA | 0.001 -> kVA |
| pf_sign_total | 54 | FLOAT, oran | 100 -> %, kapalı |
| freq | 56 | FLOAT, Hz | 1 -> Hz |
| kwh_del | 500 | INT64, Wh, Import | 0.001 -> kWh |
| kwh_rec | 504 | INT64, Wh, Export | 0.001 -> kWh |
| kvarh_del | 508 | INT64, varh, Import | 0.001 -> kVARh |
| kvarh_rec | 512 | INT64, varh, Export | 0.001 -> kVARh |
| kvah_del_rec | 516 | INT64, VAh | 0.001 -> kVAh |

Import/Export, mevcut delivered/received alanlarına yük perspektifiyle
eşlenmiştir. Bu yönler canlı karşılaştırmada doğrulanmalıdır.
INT64 enerjiler Decimal ile ölçeklenir; JSON raw_payload içinde ondalık
metin, reading_value.value_numeric içinde NUMERIC olarak tutulur.

## Sahaya özgü bilgiler

Paylaşılan ekranlarda Modbus TCP enabled=Yes, ID=100, port=502;
P1=10.123.171.46, gateway=10.123.171.40; P2=192.168.1.100 görünür.
Şablon ID'yi 100 başlatır. WAN'dan bağlanılacaksa `p_modbus_host` dış IP,
`p_modbus_port` NAT dış portu olmalıdır. Yönlendirme sahada kullanılan
arayüzün IP'si:502 hedefine yapılır; dış portun 40502 olduğu varsayılmaz.
20000, Ethernet gateway/RTU-over-TCP içindir; cihazın normal Modbus TCP
okumasıyla karıştırılmamalıdır.

`p_site_id`, gerçek seri numarası, ad, erişilebilir IP ve port doldurulmalıdır.
Yeni cihaz ID'si otomatik oluşturulur ve NOTICE mesajında gösterilir.
Mevcut cihazlar güncellenmez. Yeni cihaz status=0 ile oluşturulur.

## Anlam ve birim doğrulaması

- Ortak anlık alanlar RMS ölçümleridir. Fundamental ve High-Speed aileleri
  aynı quantity altında karıştırılmamalıdır.
- Ekrandaki kV/MW birimleri dinamik gösterimdir. Kılavuzun V/W kaynak
  birimlerine göre ölçekleme yapılır. CT/PT oranları ek olarak tahminen
  uygulanmaz; aynı zamanlı ekran karşılaştırmasıyla kontrol edilir.
- `p_enable_per_phase` true varsayılır. Kılavuza göre 3P3W bağlantıda
  faz Uln, P/Q/S ve PF registerları reserved olduğundan bu parametre false
  yapılmalıdır. Bu durumda ilgili alanlar ve Uln ortalaması kapatılır.
- PF oranı -1..1, yüzde olarak -100..100'e çevrilir. Ancak cihazın
  PF Convention ayarı (40580: IEC/IEEE/-IEEE), Schneider işaret kuralıyla
  aynı kabul edilmez. PF alanları bu nedenle kapalıdır. Büyüklüğü aynı
  olsa bile işaretin anlamı doğrulanmadan modeller arası kıyas yapılmamalıdır.
- Demand Present/Predicted/This Max/Last Max ve Max/Min kayıtları farklı
  anlam taşır. İlk şablona eklenmemiştir.
- THD, K/Crest factor, sequence unbalance ve I/O bu ilk kapsamda yoktur.
  Kılavuz s.139-141 bunların registerlarını içerir. THD'de %/x100 gösterimi,
  dengesizlikte sequence tanımı ve wiring mode doğrulanarak ayrı bir
  genişletmede eklenmelidir. Eski THD max alanlarıyla otomatik eşlenmez.
- Energy rollover 40758 ayarına bağlıdır; sayaç sıfırlama/rollover
  dönemlerinde iki okuma farkı doğrudan dönem enerjisi sayılmamalıdır.

## İlk canlı doğrulama

Önce TCP erişimi kontrol edilir. Harita oluşturulduktan ve cihazın modeli,
unit ID ve wiring mode doğrulandıktan sonra yalnızca ilgili cihaz
etkinleştirilip `python main.py SITE_ID --once` çalıştırılır.
Başka cihazın ayarını ya da ölçüm geçmişini değiştirmeyin.
Akım, gerilim, güç ve RMS Import/Export enerji değerlerini aynı zamandaki
web ekranıyla karşılaştırın. Paylaşılan ekranın farklı saniyeleri aynı
okumaymış gibi değerlendirmeyin.

Örneğin ekranın RMS bölümündeki -1.496 MW yaklaşık -1496 kW,
31.81 kV yaklaşık 31810 V ve -491.9 var yaklaşık -0.4919 kVAR olmalıdır.
PF -1.000, yüzde büyüklüğünde 100'dür; işaret politikası ayrıca doğrulanır.
Enerji ekranında RMS Import 63152.13 kWh, Export 22280835.12 kWh görünür.
Canlı değerler bu geçmiş ekran görüntüsüne eşit olmak zorunda değildir.

Canlı bağlantı veya veritabanı işlemi bu hazırlık sırasında yapılmadı.
Model adreslemesi, INT64 ve işaret/ölçek dönüşümleri çevrimdışı testlerle
kontrol edilir; bunlar sahadaki firmware ve kurulum doğrulaması yerine geçmez.
