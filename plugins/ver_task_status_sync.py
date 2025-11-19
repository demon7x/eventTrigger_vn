# :coding: utf-8

from pprint import pprint
import datetime as dt
import time
import shotgun_api3 as sa
import os
import traceback
import yaml

#global MAIN_ID
MAIN_ID = 0
DEV = 0

class SingletonInstane:
    __instance = None

    @classmethod
    def __getInstance(cls):
        return cls.__instance

    @classmethod
    def instance(cls, *args, **kargs):
        cls.__instance = cls(*args, **kargs)
        cls.instance = cls.__getInstance
        return cls.__instance

SCRIPT_PATH = '/storenext/inhouse/tool/shotgun/script/script_key.yaml'
with open( SCRIPT_PATH, 'r' ) as file:
    script_dict = yaml.load( file, Loader=yaml.FullLoader )


sg = sa.Shotgun(
                'https://vnwest.shotgrid.autodesk.com',
                api_key=script_dict['eventTrigger'],
                script_name = 'eventTrigger',
            )

# 플러그인 파일 경로 기준 설정
PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PLUGIN_DIR)
LAST_ID_DIR = os.path.join(ROOT_DIR, 'last_id')
BACKLOG_FILE = os.path.join(LAST_ID_DIR, 'ver_task_status_sync_backlog.id')

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
            print("[Ver Status Sync] Backlog processing complete. File removed.")
        except Exception as e:
            print("[Ver Status Sync] Error removing backlog file: {0}".format(e))

# -----------------------------------------------------------------------------
# Core Logic
# -----------------------------------------------------------------------------
def process_single_event(result):
    """
    단일 이벤트(result 딕셔너리)를 처리하는 핵심 로직
    """
    updated = ''

    if result and result['entity.Version.sg_status_list'] in ['change', 'di_chg' ] and not result['entity.Version.sg_task']:
        shot_result = sg.find_one(
                    'Shot',
                    [
                        ['sg_versions', 'in', result['entity']]
                    ],
                    ['code', 'id']
                )
        if shot_result:
            if not DEV:
                shot_update = sg.update( 
                        'Shot', 
                        shot_result['id'],
                        { 'sg_status_list':result['entity.Version.sg_status_list'] }
                )
            print( '[ Version -> Shot status( None Task ) ]' )
            print( '{:20} : {} / {}'.format( 
                                                'Shot Name', 
                                                result['project.Project.name'],
                                                shot_result['code']
                            )
            )
            print( '\n' )
            return

    if result and result['entity'] and result['entity.Version.sg_task']:
        if not DEV:
            updated = sg.update(
                        'Task', result['entity.Version.sg_task']['id'],
                        {'sg_status_list':result['entity.Version.sg_status_list'] }
                   )
        if updated:
            created   = result['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            page_addr = 'https://vnwest.shotgrid.autodesk.com/detail/task/{}'.format( result['entity.Version.sg_task']['id'] )
            print( '[Version status update Task status]','*'*50 )
            print( '{:15} : {}'.format( 'ID'         , result['id'] ) )
            print( '{:15} : {} / {}'.format( 'Project'    , result['project.Project.name'],
                                            result['entity']['name']  ) )
            print( '{:15} : {}'.format( 'created_at' , created ) )
            print( '{:15} : {}'.format( 'Description', result['description'] ) )
            print( page_addr )
            print( '\n' )

            if result['entity.Version.sg_status_list'] in ['dir', 'sh-dr', 'qc_rt', 'dir_ok', 'dir_rt']:
                shot_result= sg.find_one(
                        'Task', 
                        [
                            ['id', 'is', result['entity.Version.sg_task']['id'] ],
                        ],
                        ['entity','entity.Shot.code']
                        )
                if shot_result:
                    shot_update = 1
                    if not DEV:
                        shot_update = sg.update( 
                                'Shot', 
                                shot_result['entity']['id'],
                                { 'sg_status_list':result['entity.Version.sg_status_list'] }
                        )
                    if shot_update:
                        print( '[ Version -> Shot status ]' )
                        print( '{:20} : {} / {}'.format( 
                                                            'Shot Name', 
                                                            result['project.Project.name'],
                                                            shot_result['entity.Shot.code']
                                                        )
                            )
                        print( '\n' )

            elif result['entity.Version.sg_status_list'] in ['s_rt'] and result['project.Project.name'] in ['genie']:
                shot_result= sg.find_one(
                        'Task', 
                        [
                            ['id', 'is', result['entity.Version.sg_task']['id'] ],
                        ],
                        ['entity', 'entity.Shot.code', 'entity.Shot.sg_status_list']
                        )
                if shot_result and shot_result['entity.Shot.sg_status_list'] in ['fin', 'fin_d', 'dir_ok']:
                    shot_update = 1
                    if not DEV:
                        shot_update = sg.update( 
                                'Shot', 
                                shot_result['entity']['id'],
                                { 'sg_status_list':'wip' }
                        )
                    if shot_update:
                        print( '[ Version -> Shot status ] [ genie - s_rt ]' )
                        print( '{:20} : {} / {}'.format( 
                                                            'Shot Name', 
                                                            result['project.Project.name'],
                                                            shot_result['entity.Shot.code']
                                                        )
                            )
                        print( '\n' )
        else:
            print( "[ No Updated ]" )
    else:
        # pprint( result )
        # print( "entity or Version.sg_task is none type" )
        pass

def process_events_range(start_id, limit=1, label="Active"):
    """
    start_id보다 큰 이벤트를 limit만큼 가져와서 처리.
    처리된 마지막 ID를 반환. 처리할 게 없으면 start_id 반환.
    """
    filters = [
        ['attribute_name','is','sg_status_list'],
        ['event_type','is','Shotgun_Version_Change'],
        ['id', 'greater_than', start_id]
    ]
    keys = [
        'created_at','description', 'entity','project.Project.name',
        'entity.Version.sg_task',
        'entity.Version.sg_task.Task.sg_status_list',
        'entity.Version.sg_status_list',
    ]

    # start_id가 없을 경우(None/0) 오늘 날짜 기준 처리 (기존 로직 유지)
    if not start_id:
        today = dt.datetime(
            dt.datetime.now().year,
            dt.datetime.now().month,
            dt.datetime.now().day,
        )
        filters = [
            ['attribute_name','is','sg_status_list'],
            ['event_type','is','Shotgun_Version_Change'],
            ['created_at','greater_than',today]
        ]

    try:
        events = sg.find(
            'EventLogEntry', 
            filters, 
            keys,
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
        
        # 중복 처리 방지
        if start_id and current_id == start_id:
            continue

        try:
            process_single_event(event)
        except Exception as e:
            print("[{0}] Error processing event {1}: {2}".format(label, current_id, e))
            traceback.print_exc()
        
        last_processed_id = current_id

    return last_processed_id

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
def main(last_id=False):
    # 1. 최신 ID 확인 및 Gap 체크 (Active ID 관리)
    try:
        latest_event = sg.find_one("EventLogEntry", [], ["id"], order=[{"field_name": "id", "direction": "desc"}])
        if latest_event:
            latest_id = latest_event['id']
            
            # Gap이 5000 이상이면 점프 & Backlog 생성
            if last_id and (latest_id - last_id > 5000):
                print("[Ver Status Sync] Large Gap Detected ({0} -> {1}). Jumping to latest.".format(last_id, latest_id))
                
                # 이미 Backlog가 처리 중이 아니라면, 현재 last_id를 Backlog 시작점으로 저장
                if not get_backlog_id():
                    print("[Ver Status Sync] Saving current position {0} to backlog.".format(last_id))
                    set_backlog_id(last_id)
                
                # Active ID는 최신으로 점프
                return latest_id

    except Exception as e:
        print("[Ver Status Sync] Error checking latest event: {0}".format(e))

    # 2. Backlog 처리 (별도 흐름)
    backlog_id = get_backlog_id()
    if backlog_id:
        new_backlog_id = process_events_range(backlog_id, limit=10, label="Backlog")
        
        if new_backlog_id > backlog_id:
            set_backlog_id(new_backlog_id)
        
        if last_id and new_backlog_id >= last_id:
            remove_backlog_file()

    # 3. Active(Real-time) 처리
    next_active_id = process_events_range(last_id, limit=1, label="Active")
    
    return next_active_id


def main2():
    # 테스트용 함수 (사용 안 함)
    pass

if __name__ == '__main__':
    main()
