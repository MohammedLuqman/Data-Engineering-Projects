# 🏛️ Yemeni Smart Archive (YSA)
### A Geo-Distributed Real-Time Data Pipeline & OCR Processing System using Apache Spark, Kafka & Hadoop HDFS 

---

## 📌 نظرة عامة على المشروع (Project Overview)

**باللغة العربية:**
في معاملات الجهات الحكومية والقطاع الخاص في اليمن، تواجه المؤسسات تحدياً هائلاً في إدارة وأرشفة واسترجاع ملايين الوثائق الورقية والسجلات القديمة. الاعتماد على الأرشفة التقليدية يتسبب في تأخير المعاملات اليومية، وزيادة مخاطر تلف البيانات، بالإضافة إلى عزل البيانات بين الفروع جغرافياً (مثل صنعاء، عدن، تعز، الحديدة، وسيئون).

هذا المشروع يمثل حلولاً هندسية متقدمة لـ **"أزمة أرشفة تاريخية وجارية"** عبر بناء نظام متكامل يعتمد على **معمارية لامبدا (Lambda Architecture)** للجمع بين المعالجة اللحظية للبيانات (الحاضر والمستقبل) والتحليل التاريخي الضخم لحفظ وثائق الماضي من التلف وتسهيل البحث فيها بكلمة مفتاحية واحدة في أجزاء من الثانية.

**In English:**
This project addresses a critical, decades-long challenge in Yemen's public and private sectors: the digitisation, archiving, and real-time processing of national documents across geo-distributed branches (Sana'a, Aden, Taiz, Hodeidah, Seiyun). Implementing a robust **Lambda Architecture**, the system ingests legacy historical papers (Batch Layer) and instantly handles newly registered daily transactions (Speed Layer) seamlessly.

---

## 🏗️ المعمارية الهندسية ونظام التشغيل (System Architecture)

تم بناء المنظومة بالكامل كـ **End-to-End Distributed Pipeline** داخل حاويات **Docker Compose** لضمان استقرار العمل وتكامل التقنيات:

1. **Python Data Generator:** محاكاة ذكية لضخ تنبيهات المعاملات والوثائق الرسمية باللغة العربية من الفروع الجغرافية المختلفة لحظة بلحظة.
2. **Apache Kafka (KRaft Mode):** طابور بيانات فائق السرعة وموزع لاستقبال الأحداث وضمان عدم فقدان أي وثيقة تحت الضغط العالي.
3. **Apache Spark Structured Streaming:** محرك المعالجة الموزع لامتصاص البيانات وتطبيق المعالجة المتوازية.
4. **Tesseract OCR Engine:** مدمج داخل خط المعالجة للسبارك لقراءة الصور واستخراج النصوص والأسماء العربية تلقائياً وبدقة عالية.
5. **PostgreSQL (Speed Layer):** التخزين التشغيلي السريع للنصوص المستخرجة لخدمة الاستعلامات الحية الفورية.
6. **Hadoop HDFS (Batch/Historical Data Lake):** التخزين الاستراتيجي رخيص التكلفة لحفظ النسخ الأصلية الخام للوثائق بصيغة **Parquet** لحمايتها تاريخياً من التلف وتسهيل تحليلات البيانات الضخمة مستقبلاً.
7. **Grafana (Serving Layer):** لوحة تحكم ومراقبة تفاعلية حية تترجم الأكواد إلى رسومات بيانية ذكية تُحدث نفسها تلقائياً كل 5 ثوانٍ.

---

## 📂 الهيكل المالي للمشروع (Project Structure)

```text
├── 1. Python simulator/    # Python script for generating live data
├── 2. Spark streaming/     # PySpark code with integrated Tesseract OCR
├── 3. Infrastructure/      # Configuration files for Hadoop, Kafka & Grafana
├── 4. Postgres/            # SQL scripts for database and table schema
├── docker-compose.yml      # Multi-container environment orchestration
└── README.md               # System documentation & execution guide