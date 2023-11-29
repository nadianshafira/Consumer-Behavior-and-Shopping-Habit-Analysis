-- CREATE DATABASE P2M3

BEGIN;

-- Create Table table_m3
CREATE TABLE table_m3 (
    "Customer ID" SERIAL PRIMARY KEY,
    "Age" FLOAT,
    "Gender" VARCHAR(255),
    "Item Purchased" VARCHAR(255),
    "Category" VARCHAR(255),
    "Purchase Amount (USD)" INT,
    "Location" VARCHAR(255),
    "Size" VARCHAR(10),
    "Color" VARCHAR(255),
    "Season" VARCHAR(255),
    "Review Rating" FLOAT,
    "Subscription Status" VARCHAR(255),
    "Shipping Type" VARCHAR(255),
    "Discount Applied" VARCHAR(255),
    "Promo Code Used" VARCHAR(255),
    "Previous Purchases" INT,
    "Payment Method" VARCHAR(255),
    "Frequency of Purchases" VARCHAR(255)
);


COPY table_m3(
	"Customer ID",
	"Age",
	"Gender",
	"Item Purchased",
	"Category",
	"Purchase Amount (USD)",
	"Location",
	"Size",
	"Color",
	"Season",
	"Review Rating",
	"Subscription Status",
	"Shipping Type",
	"Discount Applied",
	"Promo Code Used",
	"Previous Purchases",
	"Payment Method",
	"Frequency of Purchases"
)

FROM '../files/P2M3_nadia_nabilla_data_raw.csv'
DELIMITER ','
CSV HEADER;

COMMIT;


-- Melihat Tabel table_m3
SELECT * FROM table_m3
