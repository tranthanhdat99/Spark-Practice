## 1. Kiểm tra kết nối tới Kafka server

Kiểm tra kết nối tới 3 brokers

```shell
telnet HOST1 HOST1
telnet HOST2 PORT2
telnet HOST3 PORT3 
```

Trong đó thông tin về `HOST`, `PORT` sẽ được gửi sau.

## 2. Chạy chương trình

```shell
docker container stop kafka-streaming || true &&
docker container rm kafka-streaming || true &&
docker run -ti --name kafka-streaming \
--network=streaming-network \
-p 4040:4040 \
-v ./:/spark \
-v spark_lib:/opt/bitnami/spark/.ivy2 \
-e KAFKA_BOOTSTRAP_SERVERS='HOST1:PORT1,HOST2:PORT2,HOST3:PORT3' \
-e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="USERNAME" password="PASSWORD";' \
unigap/spark:3.5 spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
/spark/11-kafka-streaming/kafka_streaming.py
```

Trong đó các thông tin về `HOST`, `PORT` và `USERNAME`, `PASSWORD` sẽ được gửi sau.

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình chuyển đổi cột `value` dạng `json` string về dạng `row` có cấu trúc và in ra kết quả convert được.

Gợi ý: Sử dụng `StructType` và hàm `from_json`

Ví dụ kết quả:

| id                                   | time_stamp | ip             | user_agent                                                                                                                                     | resolution | device_id                            | api_version | store_id | local_time          | show_recommendation | current_url                                                                                                                                                                                   | referrer_url                                                                                                     | email_address | collection                    | product_id | option                                                                                                                                                                     |
|--------------------------------------|------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------|------------|--------------------------------------|-------------|----------|---------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|---------------|-------------------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| b79f2022-fac5-4a3c-8db5-3db9865d7b01 | 1720490730 | 31.13.115.119  | facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)                                                                      | 2000x2000  | 616d29db-ae66-4ac7-9a71-e61d77f6f586 | 1.0         | 12       | 2024-07-09 09:05:30 | NULL                | https://www.glamira.fr/glamira-earring-diletta.html?fbclid=IwAR2uTEgE8b5-McEYkhj9CiSk2FA4DCDj8FNUonm4_hoDfoA2htej6KqS-3E                                                                      | https://www.facebook.com/                                                                                        |               | view_product_detail           | 98054      | [{option_label -> alloy, option_id -> 174760, value_label -> , value_id -> 1349287}, {option_label -> diamond, option_id -> 174761, value_label -> , value_id -> 1349312}] |
| f78304ed-3f81-422d-911f-275eecf234a0 | 1720490732 | 62.77.221.82   | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36                             | 1280x720   | d78cfb2f-9fb0-4922-96ef-128adeed6340 | 1.0         | 46       | 2024-07-09 09:05:32 | true                | https://www.glamira.hu/cabochon-ekszerek/                                                                                                                                                     | https://www.glamira.hu/gyemant-gyuruk/gyemant/                                                                   |               | view_listing_page             | NULL       | NULL                                                                                                                                                                       |
| 321f0e1c-ec70-425b-910c-a96882549a73 | 1720490733 | 188.221.191.42 | Mozilla/5.0 (iPhone; CPU iPhone OS 13_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) GSA/109.0.312706133 Mobile/15E148 Safari/604.1 | 414x896    | f8bc595b-e507-4dbd-9426-f5ebbbd2085e | 1.0         | 7        | 2024-07-09 09:05:33 | true                | https://www.glamira.co.uk/premium-rings/sapphire/                                                                                                                                             | https://www.glamira.co.uk/premium-rings/black-diamond/                                                           |               | view_listing_page             | NULL       | NULL                                                                                                                                                                       |
| 6b4d489c-bde1-4c5a-bf06-3f8e1a30215b | 1720490735 | 90.129.204.52  | Mozilla/5.0 (Linux; Android 9; SM-A105FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.117 Mobile Safari/537.36                     | 320x676    | 909867a4-802d-4a85-ac4d-b7d7eb29099b | 1.0         | 19       | 2024-07-09 09:05:35 | true                | https://www.glamira.se/?gclid=Cj0KCQjwlN32BRCCARIsADZ-J4v5hNKn1d5TWorsu5gNRcdc0tRy1IZJ0wF2Ls1t4dQhLJmq17R8mPcaAsuzEALw_wcB                                                                    | https://www.google.com/                                                                                          |               | search_box_action             | NULL       | NULL                                                                                                                                                                       |
| 0ab5a942-b431-4e41-9e29-98f52953a284 | 1720490736 | 84.52.226.62   | Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1      | 414x896    | 6c90309f-b390-46f8-a650-27b2b831000f | 1.0         | 30       | 2024-07-09 09:05:36 | true                | https://www.glamira.no/glamira-ring-celine-2.0crt.html?alloy=white-585&diamond=diamond-sapphire&itm_source=recommendation&itm_medium=sorting                                                  | https://www.glamira.no/forlovelses-ring/rundt-slip/carat-1.00,2.00/                                              |               | view_product_detail           | 95217      | [{option_label -> alloy, option_id -> 330189, value_label -> , value_id -> 3264100}, {option_label -> diamond, option_id -> 330188, value_label -> , value_id -> 3264046}] |
| f3403d05-2ff8-461f-84c1-38865a147136 | 1720490737 | 93.35.209.107  | Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0                                                                 | 1366x768   | e306d193-007a-4a05-af64-ae56fff8b665 | 1.0         | 14       | 2024-07-09 09:05:37 | true                | https://www.glamira.it/glamira-ring-alasha-0.8-crt.html?alloy=yellow-375&diamond=emerald&stone2=diamond-Brillant                                                                              | https://www.glamira.it/glamira-ring-alasha-1.0-crt.html?diamond=emerald&stone2=diamond-Brillant&alloy=yellow-375 |               | select_product_option_quality | 92164      | [{option_label -> stone/diamonds, option_id -> 323103, value_label -> emerald, value_id -> 2741249, quality -> AAA, quality_label -> AAA}]                                 |
| 6f1c94e3-53c9-4a35-bebe-737133402dbb | 1720490738 | 185.170.72.91  | Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36                             | 1920x1080  | dfa7a110-2f95-43f2-89f5-0647d0978445 | 1.0         | 19       | 2024-07-09 09:05:38 | true                | https://www.glamira.se/glamira-ring-cesarina.html?diamond=diamond-Brillant                                                                                                                    | https://www.glamira.se/diamantringar/diamant/                                                                    |               | select_product_option         | 98249      | [{option_label -> diamond, option_id -> 176523, value_label -> diamond-Brillant, value_id -> 1379759}]                                                                     |
| 53f0b162-e0d1-4c02-ad67-33cc4300bf21 | 1720490739 | 31.0.39.210    | Mozilla/5.0 (Linux; Android 10; SM-A705FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Mobile Safari/537.36                    | 412x915    | a87d73bd-b914-4388-a634-75a9fb689a8e | 1.0         | 50       | 2024-07-09 09:05:39 | NULL                | https://www.glamira.pl/glamira-ring-marica.html?stone2=diamond-Swarovsky&alloy=yellow-375&diamond=fire-opal&keyword=&matchtype=&gclid=EAIaIQobChMI5IT_x_Tj6AIVi46aCh01Lg4lEAEYASABEgKOrPD_BwE |                                                                                                                  |               | view_product_detail           | 97877      | [{option_label -> alloy, option_id -> 324228, value_label -> , value_id -> 2756663}, {option_label -> diamond, option_id -> 324225, value_label -> , value_id -> 2756590}] |
