import json
from requests import Response

def safe_requests_json(response: Response) -> dict:
    """
    通用清洗層：處理 BOM 與非標準編碼的 JSON 回傳值
    """
    # 1. 處理 BOM (Byte Order Mark)
    # 如果 Response 標頭沒設編碼，或設錯了，手動修正為 utf-8-sig
    if response.encoding is None or response.encoding == 'ISO-8859-1':
        response.encoding = 'utf-8-sig'
    
    try:
        # 2. 嘗試直接解析
        return response.json()
    except Exception:
        # 3. 如果失敗，手動清洗 text
        content = response.text
        # 強制移除隱形 BOM 字元
        content = content.replace('\ufeff', '').strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            # 這裡可以決定要 raise 還是 return None
            print(f"❌ Critical JSON Error: {str(e)}")
            raise