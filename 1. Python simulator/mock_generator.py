import json
import time
import random
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Kafka Connection Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'yemen_data_producer' 
}

producer = Producer(conf)
topic_name = 'yemen_archive_events'

yemeni_names = [
    "أحمد علي محسن العلفي", "فاطمة محمد حسن الشرجبي", "مقبل صالح زايد الصايدي", 
    "أروى حسين السنباني", "عبد الرقيب عبد الله الحكيمي", "بلقيس أحمد السعيدي",
    "محمد عبده ناشر المقرمي", "يسرى فضل علي اليافعي", "هشام صدام حسين الحاشدي"
]

cities = ["صنعاء", "عدن", "تعز", "الحديدة", "المكلا", "إب", "ذمار", "سيئون"]

doc_types = [
    "عقد بيع عقار ورثة", "شهادة تخرج جامعية محددة", "توثيق وكالة عامة", 
    "سجل تجاري - وزارة الصناعة", "اتفاقية توريد منظومة طاقة شمسية"
]

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

try:
    doc_counter = 1
    while True:
        current_city = random.choice(cities)
        current_owner = random.choice(yemeni_names)
        current_type = random.choice(doc_types)
        
        mock_document = {
            "document_id": f"YE-DOC-2026-{1000 + doc_counter}",
            "owner_name": current_owner,
            "document_type": current_type,
            "city": current_city,
            "issue_date": str(fake.date_between(start_date='-5y', end_date='today')),
            "metadata": {
                "archive_status": "PENDING_OCR",
                "file_path": f"/local_data/scans/doc_{1000 + doc_counter}.png"
            }
        }
        json_payload = json.dumps(mock_document, ensure_ascii=False)
        
        producer.produce(
            topic=topic_name, 
            value=json_payload.encode('utf-8'), 
            callback=delivery_report
        )
        
        producer.flush()        
        doc_counter += 1
        time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped")