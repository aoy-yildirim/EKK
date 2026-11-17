# ENTES EMK-01 entegrasyonu

Mevcut `ekk_device`, `ekk_device_register_map` ve okuma tabloları kullanılır.
Şema değişikliği yoktur. ION, iMeter ve Janitza adresleme dalları değiştirilmez.

## Dosyalar

- `insert_entes_emk01.sql`: yeni cihaz ve 55 register tanımı oluşturur.
- `tools/probe_entes_emk01.py`: yalnızca FC03 ile gerilim, frekans ve
  INT64/FLOAT enerji karşılaştırması yapar; veritabanına veya cihaza yazmaz.
- Sonraki aynı model için mevcut `clone_ekk_device.sql` kullanılabilir.

Kaynak: `EMK-01_Modbus_Register_Table.pdf`, s.1-3.
Üretici kopyası: https://www.entes.com.tr/uploads/files/EMK-01_Modbus_Register_Table.pdf
Modbus TCP ayarları: https://www.entes.com.tr/uploads/files/1700459459-EMK-01%20Turkish%20User%20Manual.pdf
Kılavuz bölüm 8.3.3: Modbus TCP etkinleştirme ve port, fabrika portu 502.

## Adresleme: ilk sahada doğrulandı

PDF FLOAT için Big Endian, long64 için 64 bit işaretli tamsayı tanımlar.
Gerilim tablosu 40001'den başlar, ancak verilen PDF'de bunun protokolde
hangi başlangıç adresine dönüştürüldüğünü gösteren örnek paket bulunmaz.
Bu nedenle sadece sayının 40001 olmasını kesin adresleme kanıtı saymayın.
17.11.2026 tarihinde kullanıcının site_id=52, device_id=56 için aldığı
probe sonucu bu cihazda PDF adreslerinin doğrudan PDU adresi olduğunu
doğruladı: offset=0. Offset 40001/40000 için bütün kontroller hata verdi;
offset=1 gerilim/enerjide hata veya anlamsız değerler üretti.

Güncel okuyucunun varsayılanı `adres = register`, yani offset=0'dır.
Haritada PDF adresleri aynen saklanır. Yalnız EMK-01 modelleri için
aşağıdaki ayarla adres çıkarımı değiştirilebilir; diğer markalar etkilenmez:

```dotenv
EKK_ENTES_EMK01_ADDRESS_OFFSET=0
```

İzin verilen değerler: 0, 1, 40000, 40001. Formül daima
`PDU adresi = PDF registerı - ayar` şeklindedir. Native adres kullanımı
bu cihazda doğrulanmıştır ve varsayılan 0'dır. Eski .env içinde 40001
varsa 0 olarak güncelleyin veya satırı kaldırıp varsayılanı kullanın.
Bu ayar aynı süreçteki tüm EMK-01 cihazları için geçerlidir. Farklı sahalarda
farklı adresleme gerekirse ayrı süreç/ortam kullanılmalıdır.

İlk olarak gerçek dış port ve unit ID ile tanılama çalıştırın:

```text
python tools/probe_entes_emk01.py 188.59.67.6 --port MODBUS_PORT --unit-id UNIT_ID
```

Cihaz zaten `ekk_device` tablosundaysa IP/port/unit ID'yi tekrar girmeden
site üzerinden çalıştırabilirsiniz (örnek site_id=52):

```text
python tools/probe_entes_emk01.py --site-id 52 --timeout 3
```

Bu mod yalnızca cihaz kaydını SELECT ile okur; pasif cihazlar da kontrol
edilebilir. EMK-01 dışındaki modellere probe uygulanmaz. Her iki kullanım
da okuma/değer/log tablolarına veya cihaz ayarlarına yazmaz.

MODBUS_PORT ve UNIT_ID yerlerini gerçek sayılarla değiştirin. Araç 4 adres
çıkarımını sırayla dener, her biri için FLOAT32 gerilim/frekans, INT64 enerji
(ham/10) ve alternatif FLOAT enerji değerlerini yazdırır. Birden fazla ağ
okuması yapar ve cevap gelmeyen adaylarda timeout bekleyebilir.
Hiçbir adayı otomatik seçmez. Hem gerilim/frekansın hem enerji sayaçlarının
web ekranıyla uyumunu kontrol edip yalnız sonra yukarıdaki ayarı kaydedin.
Sıfır veya sonlu bir sayı gelmesi tek başına doğru adres kanıtı değildir.

## İlk kapsam

55 tanım: 48 etkin, 7 kapalı PF/cos-phi.

| Grup | Adet | Durum |
|---|---:|---|
| Akım L1/L2/L3/L4 | 4 | Etkin |
| Gerilim L-N ve L-L | 6 | Etkin |
| Frekans | 1 | Etkin |
| Faz ve toplam aktif/görünür güç | 8 | Etkin |
| Tüketim/dağıtım reaktif güç (faz/toplam) | 8 | Etkin, ayrı yönler |
| Aktif ve dört yön/bileşen reaktif enerji | 6 | Etkin, INT64 |
| Anlık gerilim/akım THD | 6 | Etkin |
| K factor | 3 | Etkin, cihazın kendi tanımı |
| Pst/Plt | 6 | Etkin |
| Faz ve toplam true PF | 4 | Kapalı |
| Faz cos-phi | 3 | Kapalı, ayrı quantity |

Verilmeyen alanlar sıfır üretilmez: i5, faz ortalamaları, görünür enerji,
crest factor, toplam cos-phi, demand ve min/max ilk kapsamda yoktur.
Zaman ortalaması faz ortalaması yerine kullanılmaz. THD ölçüm penceresi
verilen tabloda belirtilmediğinden quantity son eki `_instant` seçilmiştir;
ION 3s, Janitza 200ms veya eski THD max ile otomatik eşitlenmez.

## Birimler ve anlamlar

- Akım A, gerilim V, frekans Hz olarak saklanır.
- P ve S kaynak W/VA'dır; hedef kW/kVA için çarpan 0.001'dir.
- Q tüketim ve dağıtım için ayrı verilir. `kvar_import_a/b/c/total`
  ve `kvar_export_a/b/c/total` alanları kaynak işareti korunarak kVAR'a
  dönüştürülür. `kvar_total` adı altında net Q uydurulmaz; yön/işaret
  davranışı ilk canlı kayıtta değerlendirilmelidir.
- Kaynak `long64`, okuyucuda `INT64` olarak tanımlanır. 55000 ve 55012
  için birim kWh/10'dur: `ham * 0.1 = kWh`. Wh kaynaklı diğer modellerin
  0.001 çarpanı burada kullanılmaz. İlk kayıtta FLOAT alternatifleri
  55024/55030 ve web ekranıyla çapraz kontrol yapılmalıdır.
- `kwh_del` tüketim/import (55000), `kwh_rec` dağıtım/export (55012).
- Reaktif enerji dört ayrı quantity'dir:
  `kvarh_import_inductive` 55004, `kvarh_import_capacitive` 55008,
  `kvarh_export_inductive` 55016, `kvarh_export_capacitive` 55020.
  Hepsi kaynak kvarh/10, hedef kVARh, çarpan 0.1'dir. Tek bileşen bir
  genel reaktif import/export toplamı olarak adlandırılmaz.
- INT64 Decimal hassasiyeti korunur. raw_payload JSON içinde ondalık metin,
  reading_value.value_numeric içinde NUMERIC olarak yazılır.
- Canlı probe'da tüketim INT64=1585454.6 kWh ve alternatif FLOAT=1585454.625
  kWh çıktı; 0.1 enerji ölçeği doğrulandı. Dağıtım sayacı INT64=-138.8,
  FLOAT=-138.7999878 kWh döndü. Negatif işaret cihazın iki veri biçiminde de
  mevcut; çözümleme hatası kabul edilip abs uygulanmaz. Web ekranının pozitif
  gösterimiyle aynı zamanlı karşılaştırılmadan işaret normalleştirilmez.
  Dağıtım enerjisinden dönem enerjisi hesaplayan tüketiciler bu kaynak işaretini
  dikkate almalıdır; mevcut eski kayıtlar geriye dönük değiştirilmez.
- PF ve cos-phi ayrıdır. PF için ratio->yüzde çarpanı 100 hazırlanmıştır;
  ölçek/işaret doğrulanmadan etkin değildir. Cos-phi oran olarak tutulur.
- K factor haritada birimsiz, çarpan 1 ile cihazdan geldiği gibi saklanır;
  başka üreticinin K factor hesabıyla otomatik eşdeğer kabul edilmez.
- CT/PT çarpanları tahminen ikinci kez uygulanmaz. Web ekranındaki
  sayılarla aynı ölçekte olup olmadığı doğrulanır.

## Paylaşılan cihaz ve ilk çalıştırma

Ekranda seri `19011706`, firmware `02.00.58`, yerel IP `10.135.156.7`
ve web adresi `http://188.59.67.6:40080` görünüyor. Web portundan Modbus
portu çıkarılamaz. Unit ID ekranlarda verilmemiştir; şablonda zorunlu
NULL bırakılmıştır. RS485 converter ile doğrudan cihazın Modbus TCP
servisi birbirine karıştırılmamalıdır.

1. Modbus TCP etkinliği, port, unit ID ve NAT hedefini doğrulayın.
   WAN NAT hedefi cihazın gerçek yerel IP'si:Modbus iç portudur.
2. Yukarıdaki salt okuma tanılamasıyla adresleme ve enerji ölçeğini sınayın.
3. SQL şablonunda site_id, seri, ad, IP, port, unit ID ve web URL girin.
   Model `EMK-01`, marka ENTES olarak oluşturulur. Desteklenen diğer
   yazımlar `EMK01`, `ENTES EMK-01`, `ENTES EMK01` biçimleridir.
4. Script yeni ID'leri otomatik üretir; cihazı status=0 ile oluşturur.
   Aynı site/seriyle tekrar çalıştırmak mevcut kaydı değiştirmez; UNIQUE
   hatası tüm DO bloğunu geri alır.
5. Yalnız ilgili cihaz etkinleştirilip `python main.py SITE_ID --once`
   ile tek okuma alınır. Web ekranını aynı zamanda karşılaştırın.
6. Aynı modelin doğrulanmış haritası başka sahaya clone scriptiyle
   aktarılabilir. Port ve unit ID yeni sahadan ayrıca girilir.

Ekranlardaki yaklaşık karşılaştırma noktaları (farklı saniyelerde alınmıştır):

- Gerilim yaklaşık 230 V L-N, 398-400 V L-L; akım yaklaşık 46 A.
- Aktif toplam 31.47 kW, görünür toplam 32.58 kVA.
- Cos-phi 1.000 olsa da P/S yaklaşık 0.966'dır; true PF'nin cos-phi
  yerine kullanılmaması bu ekranda özellikle önemlidir.
- Tüketim aktif enerji 1584068.0 kWh, dağıtım aktif enerji 138.2 kWh.
  Ham INT64 için beklenen yaklaşık sayılar 15840680 ve 1382'dir.
- Reaktif tüketim endüktif 718.7, kapasitif 8304.2 kVARh;
  dağıtım endüktif 22910.4, kapasitif 0.0 kVARh.
- Akım THD yaklaşık %24, gerilim THD yaklaşık %1.8. Bunlar ilk okumayla
  eşzamanlı olmadığı için birebir sayı eşitliği şart değildir.

## Doğrulama sınırları

Haritanın adres/tip/birimleri verilen PDF ile karşılaştırılır. Önceki
ION/iMeter/Janitza testleri ve EMK-01 adres çıkarımı, INT64 ölçekleme,
JSON hassasiyeti ve salt okuma probe testleri birlikte çalıştırılır.
Mock testler fiziksel cihazın adresleme biçimini veya firmware uyumunu
doğrulamaz. Kullanıcının canlı probe çıktısı adres ve enerji ölçeği için
referans testine eklendi. Agent bu güncellemede cihaza bağlanmadı veya
canlı veritabanına yazmadı. Tam haritanın poller ile okunması ayrıca kontrol
edilmelidir; probe yalnız gerilim, frekans ve aktif enerjiyi kapsar.
