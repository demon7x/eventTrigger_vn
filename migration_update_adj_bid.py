# :coding: utf-8
import os
import yaml
import shotgun_api3 as sa
import traceback
import sys

# -----------------------------------------------------------------------------
# 설정 및 초기화
# -----------------------------------------------------------------------------
SCRIPT_PATH = '/storenext/inhouse/tool/shotgun/script/script_key.yaml'
CHECKPOINT_FILE = 'migration_checkpoint.txt'  # 마지막 처리된 Task ID 저장 파일

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
        exit(1)
else:
    print("Warning: Script key file not found at {0}".format(SCRIPT_PATH))
    exit(1)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_artist_level_factor(artist_level):
    if not artist_level: return 1.0
    level = str(artist_level).lower().strip()
    if 'senior' in level: return 0.6
    elif 'mid' in level: return 1.0
    elif 'junior' in level: return 1.5
    return 1.0

def get_last_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return int(f.read().strip())
        except:
            return 0
    return 0

def save_checkpoint(task_id):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(str(task_id))

def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            print("Checkpoint cleared.")
        except:
            pass

# -----------------------------------------------------------------------------
# Main Migration Logic (Optimized with Resume)
# -----------------------------------------------------------------------------
def run_migration():
    print("Starting optimized migration script with Resume support...")

    # 0. 체크포인트 확인
    last_id = get_last_checkpoint()
    if last_id > 0:
        print("Found checkpoint! Resuming from Task ID > {0}".format(last_id))
    else:
        print("No checkpoint found. Starting from the beginning.")

    # 1. Active User 조회
    print("Fetching Active Users...")
    active_users = sg.find(
        'HumanUser',
        [['sg_status_list', 'is', 'act']],
        ['id', 'name', 'sg_artist_level']
    )
    print("Found {0} active users.".format(len(active_users)))
    user_map = {u['id']: u for u in active_users}

    # 2. Active User가 할당된 Task 조회 (last_id 이후 것만)
    print("Fetching Tasks assigned to active users...")
    
    filters = [
        ['task_assignees', 'in', active_users],
        ['time_logs_sum', 'greater_than', 0],
        ['est_in_mins', 'greater_than', 0]
    ]
    
    # 체크포인트가 있으면 ID 필터 추가
    if last_id > 0:
        filters.append(['id', 'greater_than', last_id])
    
    fields = [
        'id', 'content', 
        'task_assignees', 
        'time_logs_sum', 
        'est_in_mins', 
        'sg_timelog__ajd_bid'
    ]
    
    # ID 순으로 정렬해야 이어하기가 정확함
    tasks = sg.find('Task', filters, fields, order=[{'field_name':'id', 'direction':'asc'}])
    
    if not tasks:
        print("No more tasks to process.")
        clear_checkpoint()
        return

    print("Found {0} Tasks to process.".format(len(tasks)))
    print("-" * 100)

    count = 0
    updated_count = 0
    total_tasks = len(tasks)
    
    try:
        for task in tasks:
            count += 1
            task_id = task['id']
            task_content = task.get('content') or "No Content"
            time_logs_sum = task.get('time_logs_sum') or 0
            est_in_mins = task.get('est_in_mins') or 0
            assignees = task.get('task_assignees') or []
            
            factor = 1.0
            user_name = "Unknown"
            
            if assignees:
                first_assignee = assignees[0]
                if first_assignee['type'] == 'HumanUser':
                    user_info = user_map.get(first_assignee['id'])
                    if not user_info:
                        try:
                            user_info = sg.find_one('HumanUser', [['id', 'is', first_assignee['id']]], ['sg_artist_level', 'name'])
                        except:
                            pass
                    
                    if user_info:
                        factor = get_artist_level_factor(user_info.get('sg_artist_level'))
                        user_name = user_info.get('name')

            status_msg = "Skip"
            calc_value = 0

            try:
                # 공식 수정: 1에서 빼는 것이 아니라 비율 자체를 사용
                # 결과값은 0.0 ~ 1.0 (또는 그 이상) 사이의 실수
                raw_value = float(time_logs_sum) / (float(est_in_mins) * factor)
                
                # Percent 타입 필드는 0~100 사이의 정수값을 받음 (최대 100 제한 없음?)
                # 보통 진행률이나 사용률은 100%를 넘을 수 있으므로 min(100, ...) 제한은 정책에 따름
                # 여기서는 제한 없이 그대로 변환
                calc_value = int(raw_value * 100)
                
                current_val = task.get('sg_timelog__ajd_bid')
                
                if current_val is None or current_val != calc_value:
                    sg.update('Task', task_id, {'sg_timelog__ajd_bid': calc_value})
                    updated_count += 1
                    status_msg = "Updated"
                else:
                    status_msg = "Skip (Same)"
                    
            except ZeroDivisionError:
                status_msg = "Error (Div/0)"
            except Exception as e:
                status_msg = "Error ({0})".format(e)

            print("[{0}/{1}] Task {2:<8} ({3:<20}) | User: {4:<15} | Time: {5:>4}/{6:<4} | Fac: {7:<3} | Val: {8:>3}% | {9}".format(
                count, total_tasks, 
                task_id, 
                task_content[:20], 
                user_name[:15],    
                time_logs_sum, 
                est_in_mins, 
                factor, 
                calc_value, 
                status_msg
            ))
            sys.stdout.flush()
            
            # 처리 완료 후 체크포인트 저장
            save_checkpoint(task_id)

    except KeyboardInterrupt:
        print("\nStopped by user. Resume later from Task ID: {0}".format(get_last_checkpoint()))
        sys.exit(0)
    except Exception as e:
        print("\nUnexpected Error: {0}".format(e))
        print("Resume later from Task ID: {0}".format(get_last_checkpoint()))
        sys.exit(1)

    print("-" * 100)
    print("Migration complete. Total: {0}, Updated: {1}".format(total_tasks, updated_count))
    
    # 모든 작업 완료 시 체크포인트 삭제
    clear_checkpoint()

if __name__ == '__main__':
    run_migration()
