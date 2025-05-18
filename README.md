# Redline

Redline is a small Streamlit application used to reconcile data usage across three different sources:

- **Supplier reports**
- **Raw usage exports**
- **Customer billing files**

After uploading the three files, the app generates a single Excel workbook summarising the differences between the sources.

## Running locally

Install the dependencies listed in `requirements.txt` and start Streamlit:

```bash
pip install -r requirements.txt
streamlit run redline.py
```

This will start the web UI on http://localhost:8501/.

## Expected input

Three files are required. CSV and Excel formats are supported. Column headers may vary – the app recognises common alternatives – but the following fields must be present:

### Supplier file

- `carrier`
- `realm`
- `subscription_qty`
- `total_mb` (or `usage_mb`)

### Raw-usage file

- `date`
- `msisdn`
- `sim_serial`
- `customer_code`
- `realm`
- `carrier`
- `total_usage_mb`
- `status`

### Billing file

- `customer_code`
- `product/service`
- `qty`

The billing file may contain a few rows of headings before the actual table. The app automatically detects the header row.

## Output workbook

Once all three files are processed the app creates an Excel workbook with three sheets:

1. **Supplier_vs_Raw** – compares supplier reported usage against raw usage per carrier and realm.
2. **Supplier_vs_Cust** – compares supplier totals to customer billed amounts per realm.
3. **Raw_vs_Cust** – compares raw usage to customer billing per customer and realm.

Each sheet includes the calculated difference in megabytes and a simple status column (`OK`, `WARN`, `FAIL`) based on predefined thresholds.

