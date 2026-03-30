🚀 KIS-API-Python-Trading-Bot-Example (V22.05 Absolute Quant Engine)  
본 프로젝트는 한국투자증권(KIS) Open API를 활용하여 미국 주식 자동매매 시스템을 구축해보는 파이썬(Python) 예제 코드입니다. 이 코드는 증권사 API 통신 방법, 자율주행 스케줄러 자동화, 실시간 변동성(Volatility) 지표 분석 및 텔레그램 봇 제어 등을 학습하기 위한 최고 수준의 기술적 레퍼런스로 작성되었습니다.  
🚨 원작자 저작권 명시 및 게시 중단(Take-down) 정책 필독  
👉 본 코드에 구현된 매매 로직(무한매수법)의 모든 아이디어와 저작권, 지적재산권은 원작자인 **'라오어'**님에게 있습니다.  
👉 본 저장소는 순수하게 파이썬과 API를 공부하기 위한 기술적 예제일 뿐이며, 원작자의 공식적인 승인이나 검수를 받은 프로그램이 아닙니다.  
👉 만약 원작자(라오어님)께서 본 코드의 공유를 원치 않으시거나 삭제를 요청하실 경우, 본 저장소는 어떠한 사전 예고 없이 즉각적으로 삭제(또는 비공개 처리)될 수 있음을 명확히 밝힙니다.  
⚠️ 면책 조항 (Disclaimer)  
👉 이 코드는 한국투자증권 Open API의 기능과 파이썬 자동화 로직을 학습하기 위해 작성된 교육 및 테스트 목적의 순수 예제 코드입니다.  
👉 특정 투자 전략이나 종목을 추천하거나 투자를 권유하는 목적이 절대 아닙니다.  
👉 본 코드를 실제 투자에 적용하여 발생하는 모든 금전적 손실 및 시스템 오류에 대한 법적, 도의적 책임은 전적으로 코드를 실행한 사용자 본인에게 있습니다.  
👉 본 코드는 어떠한 형태의 수익도 보장하지 않으므로, 반드시 충분한 모의 테스트 후 본인의 책임하에 사용하시기 바랍니다.  
✨ 최신 아키텍처 주요 기술적 특징 (Key Features)  
🤖 V3.0+ 동적 변동성 스나이퍼 (Dynamic Volatility Engine) : 인간의 수동 개입을 배제하고, 야후 파이낸스 실시간 데이터를 통해 공포 지수(VXN 및 SOXX HV)의 **롤링 1년 평균(Rolling 1-Year Mean)**을 계산하여 시장 국면(Regime)에 맞춘 최적의 타격선을 매일 자율적으로 산출합니다.  
🛡️ TrueSync 장부 무결성 엔진 : KIS API 체결 내역과 로컬 장부 간의 오차 발생 시 과거 기록을 100% 보존하는 비파괴 보정(CALIB)을 수행하며, 엣지 케이스로 액면분할/병합 발생 시 봇이 스스로 감지하여 장부를 무인 소급 조정합니다.  
🔫 하이브리드 추적 사냥 (Hybrid Tracking Sniper) : 단순 지정가 맹신을 버리고, 5분봉 레이더를 통해 '바닥 대비 1.5%/1.0% 반등', '양봉 출현', '기관 매수세(거래량 MA20 돌파)'라는 3대 찐바닥 조건이 충족될 때만 낚아채는(Intercept) 트레일링 전술이 탑재되어 있습니다.  
🚦 서버 병목 및 KIS 자전거래 완벽 방어 : 호가 역전 시 -0.01달러로 강제 교정하여 자전거래 에러를 차단하며, 정규장 스케줄러에 **무작위 Jitter(0~180초 대기)**를 적용해 서버 접속 폭주(Thundering Herd) 현상을 시스템적으로 회피합니다.  
🧠 제논의 역설 타파 및 에스크로(Escrow) 락다운 : 시드 고갈 시 리버스 모드 예산의 무한 쪼개짐 버그를 수학적 역산으로 해결하고, 가상 장부 격리 금고를 통해 다중 종목 운용 시의 예산 쟁탈 및 깡통 루프를 원천 차단합니다.  
💎 텔레그램 기반 스마트 제어 : 모바일 환경에서 인라인 버튼을 통해 손쉽게 장부를 조회하고, 실시간 스나이퍼 타겟, 액면 보정, 모드 변경 등을 제어할 수 있습니다.  
🛠️ 설치 및 실행 방법 (Installation & Usage)  
📌 1. 필수 환경 (Requirements) ✔️ Python 3.12 이상  
✔️ 한국투자증권 Open API 발급 (App Key, App Secret)  
✔️ Telegram Bot Token 및 Chat ID  
📌 2. 패키지 설치 (최신 버전 엔진 구동을 위해 numpy가 추가되었습니다.)  
pip install requests yfinance pytz pandas_market_calendars python-dotenv pillow numpy "python-telegram-bot[job-queue]"  
  
📌 3. 환경 변수 설정 (.env 파일 생성) 프로젝트 최상단 폴더에 .env 파일을 만들고 아래 양식에 맞게 본인의 키를 입력합니다.  
TELEGRAM_TOKEN=나의_텔레그램_봇_토큰    
ADMIN_CHAT_ID=나의_텔레그램_채팅방_ID숫자    
APP_KEY=나의_한국투자증권_APP_KEY    
APP_SECRET=나의_한국투자증권_APP_SECRET    
CANO=나의_계좌번호_앞8자리    
ACNT_PRDT_CD=01 또는 22    
  
📌 4. 프로그램 실행  
python main.py  
  
(권장: 서버 환경에서는 nohup python main.py & 명령어를 사용하여 백그라운드에서 24시간 가동되도록 설정하세요.)  
📂 파일 구조 (Directory Structure)  
📁 main.py : 스케줄러 구동 및 프로그램의 메인 진입점(Entry Point). (자정 자가 청소 및 Jitter 스케줄링 포함)  
📁 broker.py : 한국투자증권 API 통신 및 야후 파이낸스 5분봉/일봉 데이터를 가공하는 클래스.  
📁 strategy.py : 예산 분배, 스나이퍼 감시 플래그, 리버스 생존 방어 로직이 구현된 퀀트 코어 클래스.  
📁 volatility_engine.py : [NEW] VXN 및 SOXX HV의 롤링 1년 평균을 연산하고 2중 캐시 무결성 방어망이 탑재된 동적 타격선 산출 모듈.  
📁 telegram_bot.py / telegram_view.py : 텔레그램 봇 라우터, 비동기 명령 처리 및 화면(UI) 렌더링을 담당하는 클래스.  
📁 config.py : 각종 JSON 데이터를 저장하고 불러오는 로컬 캐싱(Atomic Write 적용) 및 설정 매니저.  
📁 version_history.py : 텔레그램 페이징 한계(4096자)를 우회하여 코드 업데이트 최신 히스토리를 관리하는 기록 파일. (구 version_archive.py 통합됨)  
📁 data/ 및 logs/ : 봇 실행 시 자동 생성되며 7일 초과 시 자정(06:00)에 자동 소각되는 데이터/로그 저장소.  
```bash  

## ✨ 주요 기술적 특징 (Key Features)

💎 **Telegram 기반 스마트 UI 제어** : 텔레그램 봇 API를 연동하여 모바일에서도 인라인 버튼을 통해 손쉽게 장부를 조회하고 명령을 내릴 수 있습니다.  
💎 **안전한 KIS API 통신 및 토큰 관리** : OAuth2 기반의 KIS 인증 토큰을 로컬에 안전하게 캐싱하며, 만료 전 자동으로 갱신(Self-Healing)합니다.  
💎 **스마트 장부 동기화 엔진 (TrueSync)** : 미국장 영업일 스케줄(pandas_market_calendars)을 자동으로 인식하여, 장이 열리는 날에만 실제 API 체결 내역을 가져와 가상 장부와 실제 잔고의 오차를 완벽하게 동기화합니다.  
💎 **스케줄러 자동화 (Scheduler)** : 미국 썸머타임(DST)을 자동 감지하여 프리마켓 및 정규장 스케줄을 알아서 조정하고 실행합니다.  
💎 **모듈화된 아키텍처** : 통신(broker.py), 매매 알고리즘(strategy.py), UI(telegram_bot.py), 설정(config.py)이 객체지향적으로 완벽히 분리되어 있어 본인만의 로직으로 커스텀하기 매우 쉽습니다.

## 🛠️ 설치 및 실행 방법 (Installation & Usage)

📌 **1. 필수 환경 (Requirements)**   
✔️ Python 3.12 이상   
✔️ 한국투자증권 Open API 발급 (App Key, App Secret)   
✔️ Telegram Bot Token 및 Chat ID

📌 **2. 패키지 설치**  
pip install requests yfinance pytz pandas_market_calendars python-dotenv pillow "python-telegram-bot[job-queue]"
 

📌 3. 환경 변수 설정 (.env 파일 생성)
프로젝트 최상단 폴더에 .env 파일을 만들고 아래 양식에 맞게 본인의 키를 입력합니다.

TELEGRAM_TOKEN=나의_텔레그램_봇_토큰  
ADMIN_CHAT_ID=나의_텔레그램_채팅방_ID숫자  
APP_KEY=나의_한국투자증권_APP_KEY  
APP_SECRET=나의_한국투자증권_APP_SECRET  
CANO=나의_계좌번호_앞8자리  
ACNT_PRDT_CD=01 또는 22  

📌 4. 프로그램 실행

python main.py

(권장: 서버 환경에서는 nohup python main.py & 명령어를 사용하여 백그라운드에서 24시간 가동되도록 설정하세요.)

​📂 파일 구조 (Directory Structure)

​📁 main.py: 스케줄러 구동 및 프로그램의 메인 진입점(Entry Point)  
📁 broker.py: 한국투자증권 API 통신 및 데이터 가공을 담당하는 클래스.  
📁 strategy.py: 예산 분배 및 특정 조건에 따른 매수/매도 알고리즘이 구현된 클래스.  
📁 telegram_bot.py / telegram_view.py: 텔레그램 봇 라우터 및 화면(UI) 렌더링을 담당하는 클래스.  
📁 config.py: 각종 JSON 데이터를 저장하고 불러오는 로컬 캐싱/설정 매니저.  
📁 version_history.py: 코드 업데이트 최신 히스토리 기록  
📁 version_archive.py(삭제) 히스토리 파일 통합

```bash
