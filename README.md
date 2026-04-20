# 🚀 คู่มือการตั้งค่าระบบ: OmniCorp ELT Data Stack (One-Click Setup)

## 📌 สคริปต์ Bash นี้คืออะไร?
ไฟล์ Bash Script (`setup_omnicorp.sh`) นี้คือ **"สคริปต์อัตโนมัติ (One-Click Setup)"** ที่ออกแบบมาเพื่อลดความยุ่งยากในการเตรียม Environment สำหรับการทำ Data Engineering โดยเมื่อรันสคริปต์นี้ ตัวระบบจะทำหน้าที่:
1. สร้างโฟลเดอร์และไฟล์ที่จำเป็นทั้งหมดให้โดยอัตโนมัติ (เช่น `requirements.txt`, `etl_pipeline.py`, `Dockerfile`, `docker-compose.yml`)
2. สั่งดาวน์โหลดและเปิดใช้งาน Docker Containers ทั้งหมด (Postgres, Metabase, Prefect, Jupyter)
3. รันกระบวนการ ELT Pipeline เพื่อดูดข้อมูลเข้า Data Warehouse ให้อัตโนมัติ

---

## 🛠️ ขั้นตอนการรันระบบ (Step-by-Step Guide)

### 1. การสร้างไฟล์และวางโค้ด
คุณสามารถเลือกใช้ Text Editor ที่ถนัดได้ตามวิธีด้านล่าง:

**วิธีที่ 1: ใช้ VS Code (แนะนำสำหรับมือใหม่)**
1. เปิดโปรแกรม VS Code แล้วสร้างไฟล์ใหม่ ตั้งชื่อว่า `setup_omnicorp.sh`
2. ก๊อปปี้โค้ด Bash Script ทั้งหมดมาวางในไฟล์นี้
3. กด Save (`Ctrl + S` หรือ `Cmd + S`)

**วิธีที่ 2: ใช้ Nano (ผ่าน Terminal)**
1. เปิด Terminal (หรือ Git Bash บน Windows)
2. พิมพ์คำสั่ง: `nano setup_omnicorp.sh`
3. ก๊อปปี้โค้ด Bash Script ไปวางในหน้าจอ Nano
4. กด `Ctrl + O` แล้วกด `Enter` เพื่อ Save
5. กด `Ctrl + X` เพื่อออกจาก Nano

### 2. รันสคริปต์เพื่อสร้างระบบ
เปิด Terminal ในโฟลเดอร์ที่คุณเซฟไฟล์ `setup_omnicorp.sh` ไว้ แล้วรันคำสั่งดังนี้:

```bash
# 1. ให้สิทธิ์ระบบในการรันไฟล์ (จำเป็นสำหรับ Mac และ Linux)
chmod +x setup_omnicorp.sh

# 2. สั่งรันสคริปต์
./setup_omnicorp.sh