import requests
import pandas as pd
import re
from io import StringIO
from bs4 import BeautifulSoup

# ==============================
# 1. 노지감귤 생산량 데이터 수집
# ==============================
url = "http://www.jejugamgyul.or.kr/story/story06.asp?scrID=0000000133&pageNum=5&subNum=5&ssubNum=1"

res = requests.get(url)
res.encoding = "utf-8"
soup = BeautifulSoup(res.text, "html.parser")

tables = soup.find_all("table", {"class": "con_table"})
table = tables[3]

df = pd.read_html(StringIO(str(table)))[0]

# 숫자형으로 변환
for col in df.columns[1:]:
    df[col] = df[col].astype(str).str.replace(",", "").astype(int)

gamgyul_df = df.rename(columns={'연산별': 'YEAR'})
print("노지감귤 데이터 확인:")
print(gamgyul_df.head())

# ==============================
# 2. 기상 데이터 수집 함수
# ==============================
def fetch_weather_data(url_template, columns, rename_columns, years):
    data_list = []
    for year in years:
        url = url_template.format(year=year)
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            res.encoding = "euc-kr"
            lines = [line for line in res.text.splitlines() if not line.startswith("#") and line.strip() != ""]
            if not lines:
                print(f"{year}년 데이터 없음")
                continue

            values = re.split(r'\s+', lines[0].strip())
            values = [v.replace("=", "") for v in values if v.replace("=", "") != ""]

            if len(values) != len(columns):
                print(columns)
                print(len(columns))
                print(values)
                print(len(values))
                print(f"{year}년 데이터 열 수 불일치 ({len(values)}), 건너뜀")
                continue

            data_dict = dict(zip(columns, values))
            data_dict['YEAR'] = int(data_dict['YEAR'])  # YEAR를 바로 int로 변환
            data_list.append(data_dict)

        except Exception as e:
            print(f"{year}년 에러 발생: {e}")

    if data_list:
        df_w = pd.DataFrame(data_list)
        df_w = df_w[list(rename_columns.keys()) + ['YEAR']]
        df_w = df_w.rename(columns=rename_columns)
        # 숫자형 변환
        for col in rename_columns.values():
            df_w[col] = pd.to_numeric(df_w[col], errors='coerce')
        return df_w
    else:
        return pd.DataFrame()

# ==============================
# 3. 기상 데이터 URL & 컬럼 정의
# ==============================
weather_info = [
    {
        "name": "기온",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_ta.php?tm1={year}&tm2={year}&stn_id=184&help=1&disp=1&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'TA_YAVG', 'TMX_YAVG', 'TMN_YAVG',
                   'TMX', 'OCDT_TMX', 'TA_DAVG_MAX', 'OCDT_TA_DAVG_MAX',
                   'TMN', 'OCDT_TMN', 'TA_DAVG_MIN', 'OCDT_TA_DAVG_MIN'],
        "rename": {'TA_YAVG':'연평균기온(℃)', 'TMX':'연최고기온(℃)', 'TA_DAVG_MAX':'연최고일평균기온(℃)', 'TA_DAVG_MIN':'연최저일평균기온(℃)', 'TMN':'연최저기온(℃)'}
    },
    {
        "name": "바람",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_wind.php?tm1={year}&tm2={year}&stn_id=184&disp=1&help=0&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'WS_YAVG', 'WS_INS_MAX', 'OCDT_WS_INS_MAX', 'WD_INS_MAX', 'WS_MAX', 'OCDT_WS_MAX', 'WD_MAX', 'WD_FRQ', 'WS_MIX', 'WD_MIX'],
        "rename": {'WS_YAVG':'연평균풍속(m/s)', 'WS_INS_MAX':'최대순간풍속(m/s)', 'WS_MAX':'최대풍속(m/s)', 'WD_FRQ':'최다풍향(°)'}
    },
    {
        "name": "지면온도",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_ts.php?tm1={year}&tm2={year}&stn_id=184&disp=1&help=0&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'TS_YAVG', 'TS_MAX', 'OCDT_TS_MAX', 'TS_MIN', 'OCDT_TS_MIN'],
        "rename": {'TS_YAVG':'연평균지면온도(℃)','TS_MAX':'최고지면온도(℃)','TS_MIN':'최저지면온도(℃)'}
    },
    {
        "name": "습도",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_rhm.php?tm1={year}&tm2={year}&stn_id=184&disp=1&help=0&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'RHM_YAVG', 'RHM_MIN', 'OCDT_RHM_MIN'],
        "rename": {'RHM_YAVG':'연평균상대습도(%)','RHM_MIN':'최저상대습도(%)'}
    },
    {
        "name": "구름",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_cloud.php?tm1={year}&tm2={year}&stn_id=184&disp=1&help=0&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'LMAC_YAVG', 'TCA_YAVG', 'CA_MAX', 'OCDT_CA_MAX', 'MAX_CLFM_CD', 'MAX_CLFM_NOA', 'MIN_CH', 'OCDT_MIN_CH'],
        "rename": {'LMAC_YAVG':'평균중하층운량(hPa)','TCA_YAVG':'평균전운량(hPa)'}
    },
    {
        "name": "강수량",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_rn.php?tm1={year}&tm2={year}&stn_id=184&disp=1&help=0&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'RN_YSUM', 'RN_MAX_1HR', 'RN_MAX_1HR_OCUR_TMA', 'RN_MAX_6HR', 'RN_MAX_6HR_OCUR_TMA', 'RN_MAX_10M', 'RN_MAX_10M_OCUR_TMA'],
        "rename": {'RN_YSUM':'연합계강수량(mm)','RN_MAX_1HR':'1시간최다강수량(mm)','RN_MAX_6HR':'6시간최다강수량(mm)','RN_MAX_10M':'10분최다강수량(mm)'}
    },
    {
        "name": "안개",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_fog.php?tm1={year}&tm2={year}&stn_id=184&disp=1&help=0&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'FOG_DUR_YSUM'],
        "rename": {'FOG_DUR_YSUM':'연합계안개지속시간(시간)'}
    },
    # 일사, 일조 데이터 추가
    {
        "name": "일사",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_si.php?tm1={year}&tm2={year}&stn_id=184&help=1&disp=1&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'SI_HR1_MAX', 'OCDT_SI_HR1_MAX', 'GSR_YSUM'],
        "rename": {'SI_HR1_MAX':'최대시간당일사량(MJ/m²)','GSR_YSUM':'연합계일사량(MJ/m²)'}
    },
    {
        "name": "일조",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_ss.php?tm1={year}&tm2={year}&stn_id=184&disp=1&help=0&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",

        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'SS_HR_YSUM', 'SSRATE'],
        "rename": {'SS_HR_YSUM':'연합계일조시간(시간)','SSRATE':'연간일조율(%)'}
    },
    {
        "name": "초상온도",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_tg.php?tm1={year}&tm2={year}&stn_id=184&help=1&disp=1&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'TG_MIN_YAVG', 'TG_MIN', 'OCDT_TG_MIN'],
        "rename": {'TG_MIN_YAVG':'연평균최저초상온도(℃)', 'TG_MIN':'최저초상온도(℃)'}    
    },
    # 이슬점온도
    {
        "name": "이슬점온도",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_td.php?tm1={year}&tm2={year}&stn_id=184&help=1&disp=1&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'TD_YAVG', 'TD_MAX', 'OCDT_TD_MAX', 'TD_DAVG_MAX', 'OCDT_TD_DAVG_MAX', 'TD_MIN', 'OCDT_TD_MIN', 'TD_DAVG_MIN', 'OCDT_TD_DAVG_MIN'],
        "rename": {'TD_YAVG':'연평균이슬점온도(℃)', 'TD_MAX':'최고이슬점온도(℃)', 'TD_DAVG_MAX':'최고일평균이슬점온도(℃)', 'TD_MIN':'최저이슬점온도(℃)', 'TD_DAVG_MIN':'최저일평균이슬점온도(℃)'}
    },
    # 증발량
    {
        "name": "증발량",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_ev.php?tm1={year}&tm2={year}&stn_id=184&help=1&disp=1&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'LRG_EV_MAX', 'OCDT_LRG_EV_MAX', 'SML_EV_MAX', 'OCDT_SML_EV_MAX', 'LRG_EV_YSUM', 'SML_EV_YSUM'],
        "rename": {'LRG_EV_MAX':'최대대형증발량(mm)', 'SML_EV_MAX':'최대소형증발량(mm)', 'LRG_EV_YSUM':'연합계대형증발량(mm)', 'SML_EV_YSUM':'연합계소형증발량(mm)'}
    },
    # 시정
    {
        "name": "시정",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_vs.php?tm1={year}&tm2={year}&stn_id=184&help=1&disp=1&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'VS_MIN', 'OCDT_VS_MIN'],
        "rename": {'VS_MIN':'최소시정(m)'}  
    },    
    # 증기압
    {
        "name": "증기압",
        "url": "https://apihub.kma.go.kr/api/typ01/url/sts_pv.php?tm1={year}&tm2={year}&stn_id=184&help=1&disp=1&authKey=ldCz7wQOQ8iQs-8EDhPIoQ",
        "columns": ['YEAR', 'STN_ID', 'LAT', 'LON', 'ALTD', 'PV_YAVG', 'PV_MAX', 'OCDT_PV_MAX', 'PV_MIN', 'OCDT_PV_MIN'],
        "rename": {'PV_YAVG':'연평균증기압(hPa)', 'PV_MAX':'최고증기압(hPa)', 'PV_MIN':'최저증기압(hPa)'}
    }
]

# ==============================
# 4. 수집 및 병합
# ==============================
all_weather_dfs = []
years = gamgyul_df['YEAR']

for info in weather_info:
    print(f"{info['name']} 데이터 수집 중...")
    df_w = fetch_weather_data(info['url'], info['columns'], info['rename'], years)
    if not df_w.empty:
        all_weather_dfs.append(df_w)

merged_df = gamgyul_df.copy()
for df_w in all_weather_dfs:
    merged_df = pd.merge(merged_df, df_w, on='YEAR', how='left')

print("\n통합 데이터 확인:")
print(merged_df.head())

# ==============================
# 5. 통합 데이터 파일로 저장
# ==============================
merged_df.to_csv("gamgyul_weather_merged.csv", index=False, encoding='utf-8-sig')
print("통합 데이터를 'gamgyul_weather_merged.csv'로 저장 완료!")

# 이후에는 다시 수집하지 않고 불러오기 가능
# merged_df = pd.read_csv("gamgyul_weather_merged.csv")