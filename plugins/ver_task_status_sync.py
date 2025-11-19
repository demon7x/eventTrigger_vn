# :coding: utf-8

from pprint import pprint
import datetime as dt
import time
import shotgun_api3 as sa
import os

import yaml
#from rocketchat.api import RocketChatAPI
#from rocketchat_API.rocketchat import RocketChat

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


# def set_status_id( _id ):
#     with open( './last_id/ver_status_id.tx' , 'w' ) as f:
#         f.write( str(_id) )
#
# def get_status_id( ):
#     status_file = './last_id/ver_status_id.tx'
#     if not os.path.exists( status_file ):
#
#         return False
#     with open( status_file ) as f:
#         result = f.read()
#     print 'result id : ', result
#     return int(result)

#def send_rchat_msg( content , task ):
#    api = RocketChat(
#                    settings={
#                        'username':'shotgun@west.co.kr',
#                        'password':'west',
#                        'domain':'http://10.0.20.73:3000'
#                        }
#                )
#
#    user = ''
#    if 'anim' in task :
#        user = u'@Animation_윤호근' 
#    elif 'rig' in task:
#        user = u'@Rigging_전병근'
#    elif 'sim' in task:
#        user = u'@Rigging_전병근'
#
#    if not user:
#        return
#
#    room = api.create_im_room( user )
#    api.send_message( u'Status가 tel로 변경되었습니다.', room['id'] )
#    api.send_message( content, room['id'] )
#    print( "\n[ Rockec Chat ] Sending message\n" )


def sync_version_to_task( old_id ):
#    if MAIN_ID == old_id:
#        print "Previous ID is same with old id"
#        return

    filters = [
        ['attribute_name','is','sg_status_list'],
        ['event_type','is','Shotgun_Version_Change'],
    ]
    keys = [
        'created_at','description', 'entity','project.Project.name',
        'entity.Version.sg_task',
        'entity.Version.sg_task.Task.sg_status_list',
        'entity.Version.sg_status_list',
    ]
    if not old_id:
        today = dt.datetime(
            dt.datetime.now().year,
            dt.datetime.now().month,
            dt.datetime.now().day,
        )
        filters.append( ['created_at','greater_than',today] )
    else:
        filters.append( ['id', 'greater_than', old_id ] )

    result = sg.find_one(
        'EventLogEntry', filters , keys
    )
    if not result:
        result = {}
        result['id'] = old_id
        return old_id
    #for result in results:
    if old_id == result['id']:
        print( "Previous ID is same with old id" )
        result = {}
        result['id'] = old_id
        return old_id
    # pprint( result )
    # print '\n'

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

            return result['id']

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

#            if result['entity.Version.sg_task.Task.sg_status_list'] == 'tel':
#                send_rchat_msg( page_addr, result['entity.Version.sg_task'] )

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

            return result['id']
        else:
            print( "[ No Updated ]" )
            return result['id']
    else:
        pprint( result )
        print( "entity or Version.sg_task is none type" )
        return result['id']

def main( last_id = False ):
    return sync_version_to_task( last_id )


def main2():
    old_id = get_status_id()
    #old_id = False
    

#    while True:        
#        time.sleep( 10 )
#        result = sync_version_to_task( old_id )
#        if result == 'No':
#            continue
#        set_status_id( result['id'] )
#        old_id = result['id']


    while True:
        try:
            time.sleep( 10 )
            result = sync_version_to_task( old_id )
            if result == 'No':
                continue
            set_status_id( result['id'] )
            old_id = result['id']
        except KeyboardInterrupt:
            break
        except:
            print( '\n' )
            print( '[ Error  ]',time.strftime( '%Y-%m-%d %H:%S', time.localtime() ) )
            print( '[ old id ]',old_id )
           #pprint( result )


        # try:
        #     time.sleep( 5 )
        #     result = sync_version_to_task( old_id )
        #     set_status_id( result['id'] )
        #     old_id = result['id']
        # except:
        #     old_id = get_status_id()
        #     print '\n'
        #     print '[ Error ]'



if __name__ == '__main__':
    main()
