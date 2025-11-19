# :coding: utf-8
import os
import yaml
import shotgun_api3 as sa
import traceback

# -----------------------------------------------------------------------------
# 설정 및 초기화
# -----------------------------------------------------------------------------
SCRIPT_PATH = '/storenext/inhouse/tool/shotgun/script/script_key.yaml'
sg = None

# 플러그인 파일 경로 기준 설정
PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PLUGIN_DIR)
LAST_ID_DIR = os.path.join(ROOT_DIR, 'last_id')
BACKLOG_FILE = os.path.join(LAST_ID_DIR, 'calc_task_adj_bid_backlog.id')

if os.path.exists(SCRIPT_PATH):
    try:
        with open(SCRIPT_PATH, 'r') as file:
            script_dict = yaml.load(file, Loader=yaml.FullLoader)
        
        sg = sa.Shotgun(
            'https://vnwest.shotgrid.autodesk.com',
            api_key=script_dict['eventTrigger'],
            script_name='eventTrigger',
        )
    except Exception as e:
        print("Error initializing Shotgun connection: {0}".format(e))
else:
    print("Warning: Script key file not found at {0}".format(SCRIPT_PATH))

# -----------------------------------------------------------------------------
# Helper Functions (Backlog ID Management)
# -----------------------------------------------------------------------------
def get_backlog_id():
    if not os.path.exists(BACKLOG_FILE):
        return None
    try:
        with open(BACKLOG_FILE, 'r') as f:
            return int(f.read().strip())
    except:
        return None

def set_backlog_id(event_id):
    if not os.path.exists(LAST_ID_DIR):
        os.makedirs(LAST_ID_DIR)
    with open(BACKLOG_FILE, 'w') as f:
        f.write(str(event_id))

def remove_backlog_file():
    if os.path.exists(BACKLOG_FILE):
        try:
            os.remove(BACKLOG_FILE)
            print("[Calc Adj Bid] Backlog processing complete. File removed.")
        except Exception as e:
            print("[Calc Adj Bid] Error removing backlog file: {0}".format(e))

# -----------------------------------------------------------------------------
# Core Logic
# -----------------------------------------------------------------------------
def get_artist_level_factor(artist_level):
    if not artist_level:
        return 1.0
    level = str(artist_level).lower().strip()
    if 'senior' in level: return 0.6
    elif 'mid' in level: return 1.0
    elif 'junior' in level: return 1.5
    return 1.0

def update_task_adj_bid(task_id, timelog_user):
    if not sg: return

    try:
        task = sg.find_one(
            'Task',
            [['id', 'is', task_id]],
            ['time_logs_sum', 'est_in_mins', 'sg_timelog__ajd_bid']
        )
        
        if not task: return

        time_logs_sum = task.get('time_logs_sum') or 0
        est_in_mins = task.get('est_in_mins') or 0

        if est_in_mins == 0: return

        factor = 1.0
        if timelog_user and timelog_user['type'] == 'HumanUser':
            user = sg.find_one('HumanUser', [['id', 'is', timelog_user['id']]], ['sg_artist_level'])
            if user:
                factor = get_artist_level_factor(user.get('sg_artist_level'))

        try:
            raw_value = float(time_logs_sum) / (float(est_in_mins) * factor)
            calc_value = int(raw_value * 100)
            
            current_val = task.get('sg_timelog__ajd_bid')
            
            if current_val is None or current_val != calc_value:
                sg.update('Task', task_id, {'sg_timelog__ajd_bid': calc_value})
                print("[Calc Adj Bid] Task {0} Updated: {1}% (Raw: {2:.4f}, Time: {3}, Est: {4}, Factor: {5}, User: {6})".format(
                    task_id, calc_value, raw_value, time_logs_sum, est_in_mins, factor, timelog_user.get('name')))
                
        except ZeroDivisionError:
            print("[Calc Adj Bid] Task {0}: Division by zero.".format(task_id))

    except Exception as e:
        print("[Calc Adj Bid] Error updating Task {0}: {1}".format(task_id, e))
        traceback.print_exc()

def process_events_range(start_id, limit=1, label="Active"):
    """
    start_id보다 큰 이벤트를 limit만큼 가져와서 처리.
    처리된 마지막 ID를 반환. 처리할 게 없으면 start_id 반환.
    """
    if not sg: return start_id

    event_types = ['Shotgun_TimeLog_New', 'Shotgun_TimeLog_Change']
    filters = [
        ['id', 'greater_than', start_id],
        ['event_type', 'in', event_types]
    ]
    
    try:
        events = sg.find(
            'EventLogEntry',
            filters,
            ['id', 'event_type', 'entity'],
            order=[{'field_name': 'id', 'direction': 'asc'}],
            limit=limit
        )
    except Exception as e:
        print("[{0}] Error finding events: {1}".format(label, e))
        return start_id

    if not events:
        return start_id

    last_processed_id = start_id
    
    for event in events:
        current_id = event['id']
        try:
            if event['entity'] and event['entity']['type'] == 'TimeLog':
                timelog_id = event['entity']['id']
                timelog = sg.find_one('TimeLog', [['id', 'is', timelog_id]], ['entity', 'user'])
                
                if timelog and timelog.get('entity') and timelog['entity']['type'] == 'Task':
                    task_id = timelog['entity']['id']
                    timelog_user = timelog.get('user')
                    update_task_adj_bid(task_id, timelog_user)
        except Exception as e:
            print("[{0}] Error processing event {1}: {2}".format(label, current_id, e))
            traceback.print_exc()
        
        last_processed_id = current_id

    return last_processed_id

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
def main(last_id):
    if not sg: return last_id

    # 1. 최신 ID 확인 및 Gap 체크 (Active ID 관리)
    try:
        latest_event = sg.find_one("EventLogEntry", [], ["id"], order=[{"field_name": "id", "direction": "desc"}])
        if latest_event:
            latest_id = latest_event['id']
            
            # Gap이 5000 이상이면 점프 & Backlog 생성
            if not last_id or (latest_id - last_id > 5000):
                print("[Calc Adj Bid] Large Gap Detected ({0} -> {1}). Jumping to latest.".format(last_id, latest_id))
                
                # 이미 Backlog가 처리 중이 아니라면, 현재 last_id를 Backlog 시작점으로 저장
                if last_id and not get_backlog_id():
                    print("[Calc Adj Bid] Saving current position {0} to backlog.".format(last_id))
                    set_backlog_id(last_id)
                
                # Active ID는 최신으로 점프 (여기서 리턴하면 main_trigger가 last_id를 갱신함)
                return latest_id

    except Exception as e:
        print("[Calc Adj Bid] Error checking latest event: {0}".format(e))

    # 2. Backlog 처리 (별도 흐름)
    # Active 처리와 무관하게 매 루프마다 조금씩(10개씩) 처리
    backlog_id = get_backlog_id()
    if backlog_id:
        # Active ID(last_id)를 넘지 않도록 주의 (안전장치)
        # 하지만 Active ID는 이미 점프했을 수 있으므로, 단순히 처리만 진행
        new_backlog_id = process_events_range(backlog_id, limit=10, label="Backlog")
        
        if new_backlog_id > backlog_id:
            set_backlog_id(new_backlog_id)
            # print("[Calc Adj Bid] Backlog processed up to {0}".format(new_backlog_id))
        
        # Backlog가 Active ID(현재 처리 중인 last_id)를 따라잡았거나, 더 이상 처리할 게 없어서
        # 최신 이벤트 근처에 도달했는지 확인하는 로직은 복잡하므로,
        # 여기서는 단순히 "더 이상 처리할 이벤트가 없으면(find 결과가 없으면)" 삭제하는 방식은 위험함 (이벤트가 드물게 발생할 수 있음).
        # 따라서 Active ID와 비교해야 함.
        if last_id and new_backlog_id >= last_id:
            remove_backlog_file()

    # 3. Active(Real-time) 처리
    # main_trigger.py는 여기서 반환된 ID를 다음 루프의 last_id로 사용함
    # 한 번에 1개씩 처리 (기존 방식 유지)
    next_active_id = process_events_range(last_id, limit=1, label="Active")
    
    return next_active_id
