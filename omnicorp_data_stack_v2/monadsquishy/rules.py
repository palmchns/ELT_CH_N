# -*- coding: utf-8 -*-
"""
Monasquishy Rules - Validators & Transformers for Chinook Pipeline
"""

import re
import pandas as pd
from dateutil import parser as dateutil_parser
from .monadsquishy import validator, transformer

# ==========================================
# 🛑 VALIDATORS (ตรวจสอบ - ถ้าผิดจะ raise Exception)
# ==========================================

@validator
def must_exist(v):
    """ห้ามเป็นค่าว่าง"""
    if pd.isna(v) or str(v).strip() == '' or str(v).lower() in ['n/a', 'null', 'nan', 'none', '<na>', '-', 'ไม่ระบุ']:
        raise ValueError("Missing")
    return v

@validator
def validate_numeric_only(v):
    """ต้องเป็นตัวเลขเท่านั้น (รองรับทศนิยมและค่าติดลบ)"""
    s = str(v).strip()
    if not re.match(r'^-?\d+(\.\d+)?$', s):
        raise ValueError(f"Not a valid number: '{v}'")
    return v

@validator
def validate_optional_whole_number(v):
    """Validate an optional positive integer source ID."""
    if pd.isna(v):
        return v
    number = pd.to_numeric(v, errors="coerce")
    if pd.isna(number) or number <= 0 or number % 1 != 0:
        raise ValueError(f"Invalid source ID: '{v}'")
    return v

@validator
def validate_positive(v):
    if pd.isna(v):
        return v
    if float(v) <= 0:
        raise ValueError(f"Expected positive value: '{v}'")
    return v

@validator
def validate_non_negative(v):
    if pd.isna(v):
        return v
    if float(v) < 0:
        raise ValueError(f"Expected non-negative value: '{v}'")
    return v

@validator
def validate_unit_interval(v):
    if pd.isna(v):
        return v
    if not 0 <= float(v) <= 1:
        raise ValueError(f"Expected value between 0 and 1: '{v}'")
    return v

@validator
def validate_email(v):
    """ตรวจสอบรูปแบบ Email"""
    if pd.isna(v) or str(v).strip() == "":
        return v
    s = str(v).strip()
    pattern = r"^[^\s@]+@[^\s@]+\.[^\s@]+$"
    if not re.fullmatch(pattern, s):
        raise ValueError(f"Invalid email: '{v}'")
    return v

@validator
def validate_contact_number(v):
    """Check international/local phone or fax syntax without changing the original number."""
    if pd.isna(v) or str(v).strip() == "":
        return v
    value = str(v).strip()
    digits = re.sub(r"\D", "", value)
    if not re.fullmatch(r"[+()0-9.\s/-]+", value) or not 7 <= len(digits) <= 15:
        raise ValueError(f"Invalid contact number: '{v}'")
    return v

@validator
def validate_url(v):
    """ตรวจสอบรูปแบบ URL"""
    s = str(v).strip()
    pattern = r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$"
    if not re.match(pattern, s):
        raise ValueError(f"Invalid URL: '{v}'")
    return s

@validator
def validate_ip_address(v):
    """ตรวจสอบรูปแบบ IPv4"""
    s = str(v).strip()
    pattern = r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"
    if not re.fullmatch(pattern, s):
        raise ValueError(f"Invalid IPv4: '{v}'")
    return s

@validator
def validate_boolean_format(v):
    """ตรวจสอบว่าเป็น True/False (Y/N, 1/0, T/F)"""
    s = str(v).strip().lower()
    if s not in ['true', 'false', '1', '0', 'y', 'n', 'yes', 'no', 't', 'f']:
        raise ValueError(f"Not a boolean: '{v}'")
    return v

@validator
def validate_post_code_th(v):
    """ตรวจสอบรหัสไปรษณีย์ไทย (5 หลัก ไม่ขึ้นด้วย 0)"""
    s = str(v).strip()
    if not re.fullmatch(r"^[1-9][0-9]{4}$", s):
        raise ValueError(f"Invalid Thai post code: '{v}'")
    return s

@validator
def validate_thai_citizen_id(v):
    """ตรวจสอบเลขบัตรประชาชนไทย 13 หลัก พร้อม Checksum"""
    s = str(v).strip()
    if not re.fullmatch(r"^[0-9]{13}$", s):
        raise ValueError(f"Citizen ID must be 13 digits: '{v}'")
    total = sum(int(s[i]) * (13 - i) for i in range(12))
    check_digit = (11 - (total % 11)) % 10
    if check_digit != int(s[12]):
        raise ValueError(f"Invalid Citizen ID checksum: '{v}'")
    return s


# ==========================================
# 🛠️ TRANSFORMERS (แปลงค่า - ถ้าสำเร็จจะหยุด chain)
# ==========================================

@transformer
def normalize_whitespace(v):
    """ลบช่องว่างหัวท้าย และยุบ space เกิน"""
    if pd.isna(v): raise ValueError("NA")
    return re.sub(r"\s+", " ", str(v)).strip()

@transformer
def normalize_optional_whitespace(v):
    """Normalize text while preserving missing values without failure."""
    if pd.isna(v):
        return v
    return re.sub(r"\s+", " ", str(v)).strip()

@transformer
def transform_to_lowercase(v):
    """แปลงเป็นตัวพิมพ์เล็ก"""
    if pd.isna(v): raise ValueError("NA")
    return str(v).lower().strip()

@transformer
def transform_to_uppercase(v):
    """แปลงเป็นตัวพิมพ์ใหญ่"""
    if pd.isna(v): raise ValueError("NA")
    return str(v).upper().strip()

@transformer
def transform_to_titlecase(v):
    """แปลงตัวอักษรตัวแรกของทุกคำเป็นพิมพ์ใหญ่"""
    if pd.isna(v): raise ValueError("NA")
    return str(v).title().strip()

@transformer
def remove_special_chars(v):
    """ลบอักขระพิเศษ เหลือแค่ตัวอักษร ตัวเลข ภาษาไทย"""
    if pd.isna(v): raise ValueError("NA")
    return re.sub(r'[^A-Za-z0-9ก-๙\s]', '', str(v)).strip()

@transformer
def remove_html_tags(v):
    """ลบ HTML Tags ออกจาก Text"""
    if pd.isna(v): raise ValueError("NA")
    return re.sub(r'<[^>]+>', '', str(v)).strip()

@transformer
def extract_only_numbers(v):
    """ดึงเฉพาะตัวเลขออกมา"""
    if pd.isna(v): raise ValueError("NA")
    nums = re.sub(r'\D', '', str(v))
    if not nums: raise ValueError("No digits found")
    return nums

@transformer
def currency_to_float(v):
    """แปลงค่าเงิน (฿, $, ,) ให้เป็น Float"""
    if pd.isna(v): raise ValueError("NA")
    s = re.sub(r'[^\d.-]', '', str(v))
    return float(s)

@transformer
def normalize_boolean(v):
    """แปลง Y/N, 1/0 ให้เป็น 'True'/'False' มาตรฐาน"""
    if pd.isna(v): raise ValueError("NA")
    s = str(v).strip().lower()
    if s in ['true', '1', 'y', 'yes', 't']:
        return "True"
    elif s in ['false', '0', 'n', 'no', 'f']:
        return "False"
    raise ValueError(f"Cannot normalize boolean: '{v}'")

@transformer
def to_float(v):
    """แปลงเป็น float (ลบ comma, $, ฿)"""
    return float(str(v).replace(',', '').replace('$', '').replace('฿', ''))

# --- Date Parsers (แบบ chain: ลองหลาย format ถ้าตัวแรกพัง จะไปลองตัวถัดไป) ---
@transformer
def parse_iso(v):
    """Parse วันที่รูปแบบ ISO: YYYY-MM-DD"""
    return pd.to_datetime(v, format='%Y-%m-%d')

@transformer
def parse_us(v):
    """Parse วันที่รูปแบบ US: MM/DD/YYYY"""
    return pd.to_datetime(v, format='%m/%d/%Y')

@transformer
def parse_uk(v):
    """Parse วันที่รูปแบบ UK/Thai: DD/MM/YYYY"""
    return pd.to_datetime(v, format='%d/%m/%Y')

@transformer
def parse_thai(v):
    """Parse วันที่แบบ พ.ศ.: 2567-01-01 -> 2024-01-01"""
    s = str(v)
    parts = s.split('-')
    if len(parts) == 3 and parts[0].isdigit() and int(parts[0]) > 2400:
        return pd.to_datetime(f"{int(parts[0])-543}-{parts[1]}-{parts[2]}")
    raise ValueError("Not Thai Format")

@transformer
def flexible_date_parser(v):
    """Parse วันที่แบบยืดหยุ่น (ใช้ dateutil)"""
    if pd.isna(v) or str(v).strip() == '':
        raise ValueError("Empty")
    s = str(v).strip()
    if re.match(r'^\d{4}-\d{1,2}-\d{1,2}(?:$|[ T])', s):
        year = int(s[:4])
        if year > 2400:
            s = f"{year - 543}{s[4:]}"
        dt = pd.to_datetime(s, errors='raise')
    else:
        dt = dateutil_parser.parse(s, dayfirst=True)
    return dt.strftime('%Y-%m-%d')

# --- Phone & Name ---
@transformer
def extract_multiple_phones(v):
    """แยกเบอร์โทรหลายเบอร์ คืนค่าเป็น comma-separated"""
    if pd.isna(v) or str(v).strip() == '':
        raise ValueError("Empty")
    raw_phones = re.split(r'[,/]|ต่อ', str(v))
    valid_phones = []
    for phone in raw_phones:
        s = re.sub(r'\D', '', phone)
        if s.startswith("66") and len(s) >= 9:
            s = "0" + s[2:]
        if s.isdigit() and len(s) in (9, 10) and s.startswith("0"):
            valid_phones.append(s)
    if not valid_phones:
        raise ValueError("No valid phone found")
    return ",".join(valid_phones)

@transformer
def split_full_name(v):
    """แยก First/Middle/Last name เป็น JSON string"""
    if pd.isna(v) or str(v).strip() == '':
        raise ValueError("Empty")
    clean_name = re.sub(r'\s+', ' ', str(v).strip())
    parts = clean_name.split(' ')
    result = {"first_name": parts[0], "middle_name": None, "last_name": None}
    if len(parts) == 2:
        result["last_name"] = parts[1]
    elif len(parts) >= 3:
        result["middle_name"] = " ".join(parts[1:-1])
        result["last_name"] = parts[-1]
    return json.dumps(result, ensure_ascii=False)

# --- ISO Code ---
@transformer
def to_iso_code(v):
    """แปลงชื่อประเทศเป็น ISO Code"""
    m = {'TH': 'TH', 'THAILAND': 'TH', 'USA': 'US', 'UNITED STATES': 'US',
         'UK': 'UK', 'UNITED KINGDOM': 'UK', 'JP': 'JP', 'JAPAN': 'JP',
         'CN': 'CN', 'CHINA': 'CN'}
    res = m.get(str(v).strip().upper(), None)
    if res is None:
        raise ValueError("Mapping failed")
    return res


# ==========================================
# 🏭 FACTORY RULES (กฎที่รับ Parameter ได้)
# ==========================================

def validate_length(min_len, max_len):
    """ตรวจสอบความยาวข้อความ"""
    @validator
    def rule(v):
        s = str(v).strip()
        if not (min_len <= len(s) <= max_len):
            raise ValueError(f"Length {len(s)} not between {min_len}-{max_len}")
        return v
    rule.__name__ = f"validate_length_{min_len}_{max_len}"
    return rule

def validate_in_list(allowed_values: list):
    """ตรวจสอบว่าค่าอยู่ใน List ที่อนุญาต"""
    @validator
    def rule(v):
        if v not in allowed_values:
            raise ValueError(f"'{v}' not in allowed list")
        return v
    rule.__name__ = "validate_in_list"
    return rule
