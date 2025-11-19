# :coding: utf-8
import os
import yaml
import shotgun_api3 as sa
import traceback

# -----------------------------------------------------------------------------
# 설정 및 초기화
# -----------------------------------------------------------------------------
# 기존 플러그인(ver_task_status_sync.py)과 동일한 경로 사용
SCRIPT_PATH = '/storenext/inhouse/tool/shotgun/script/script_key.yaml'
sg = None

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
    # 로컬 테스트 환경 등 파일이 없을 경우 경고 메시지
    print("Warning: Script key file not found at {0}".format(SCRIPT_PATH))

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_artist_level_factor(artist_level):
    """
    sg_artist_level 텍스트 값을 보정치로 변환
    Senior : 0.6
    Mid : 1.0
    Junior : 1.5
    """
    if not artist_level:
        return 1.0
    
    level = str(artist_level).lower().strip()
    
    if 'senior' in level:
        return 0.6
    elif 'mid' in level:
        return 1.0
    elif 'junior' in level:
        return 1.5
    
    return 1.0

def update_task_adj_bid(task_id, timelog_user):
    """
    Task의 time_logs_sum과 TimeLog 생성자(User)의 레벨을 기반으로 sg_timelog__ajd_bid 업데이트
    공식: 1 - time_logs_sum / (est_in_mins * 보정치)
    """
    if not sg:
        return

    try:
        # Task 정보 조회 (time_logs_sum, est_in_mins)
        task = sg.find_one(
            'Task',
            [['id', 'is', task_id]],
            ['time_logs_sum', 'est_in_mins', 'sg_timelog__ajd_bid']
        )
        
        if not task:
            return

        time_logs_sum = task.get('time_logs_sum') or 0
        est_in_mins = task.get('est_in_mins') or 0

        # est_in_mins가 0이면 계산 불가 (0으로 나누기 방지)
        if est_in_mins == 0:
            return

        # 보정치 계산 (TimeLog 생성자 기준)
        factor = 1.0
        if timelog_user and timelog_user['type'] == 'HumanUser':
            user = sg.find_one(
                'HumanUser', 
                [['id', 'is', timelog_user['id']]], 
                ['sg_artist_level']
            )
            if user:
                factor = get_artist_level_factor(user.get('sg_artist_level'))

        # 공식 적용
        try:
            # time_logs_sum과 est_in_mins는 분 단위
            calc_value = 1.0 - (float(time_logs_sum) / (float(est_in_mins) * factor))
            
            # 기존 값과 다를 경우에만 업데이트 (불필요한 API 호출 방지)
            current_val = task.get('sg_timelog__ajd_bid')
            
            # 값이 없거나 차이가 있을 때 업데이트 (소수점 4자리 정도 차이)
            if current_val is None or abs(float(current_val) - calc_value) > 0.0001:
                sg.update('Task', task_id, {'sg_timelog__ajd_bid': calc_value})
                print("[Calc Adj Bid] Task {0} Updated: {1:.4f} (Time: {2}, Est: {3}, Factor: {4}, User: {5})".format(
                    task_id, calc_value, time_logs_sum, est_in_mins, factor, timelog_user.get('name')))
                
        except ZeroDivisionError:
            print("[Calc Adj Bid] Task {0}: Division by zero during calculation.".format(task_id))

    except Exception as e:
        print("[Calc Adj Bid] Error updating Task {0}: {1}".format(task_id, e))
        traceback.print_exc()

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
def main(last_id):
    if not sg:
        return last_id

    # TimeLog 변경 감지 (New, Change)
    # Task의 time_logs_sum은 TimeLog가 추가되거나 변경될 때 변동됨
    event_types = ['Shotgun_TimeLog_New', 'Shotgun_TimeLog_Change']
    
    filters = [
        ['id', 'greater_than', last_id],
        ['event_type', 'in', event_types]
    ]
    
    # 이벤트 조회 (ID 순으로 하나만)
    event = sg.find_one(
        'EventLogEntry',
        filters,
        ['id', 'event_type', 'entity'],
        order=[{'field_name': 'id', 'direction': 'asc'}]
    )
    
    if not event:
        return last_id

    current_id = event['id']
    
    try:
        # TimeLog 이벤트 처리
        if event['entity'] and event['entity']['type'] == 'TimeLog':
            timelog_id = event['entity']['id']
            
            # TimeLog가 연결된 Task와 User(생성자) 찾기
            timelog = sg.find_one('TimeLog', [['id', 'is', timelog_id]], ['entity', 'user'])
            
            if timelog and timelog.get('entity') and timelog['entity']['type'] == 'Task':
                task_id = timelog['entity']['id']
                timelog_user = timelog.get('user')
                update_task_adj_bid(task_id, timelog_user)
                
    except Exception as e:
        print("[Calc Adj Bid] Error processing event {0}: {1}".format(current_id, e))
        traceback.print_exc()

    return current_id
