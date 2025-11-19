# :coding: utf-8

from multiprocessing import Process
from pprint import pprint
import datetime as dt
import shutil
import time
import shotgun_api3 as sa
import os

import yaml
#from rocketchat.api import RocketChatAPI

#global MAIN_ID
MAIN_ID = 0
DEV = 0


SCRIPT_PATH = '/storenext/inhouse/tool/shotgun/script/script_key.yaml'
with open( SCRIPT_PATH, 'r' ) as file:
    script_dict = yaml.load( file, Loader=yaml.FullLoader )


sg = sa.Shotgun(
                'https://vnwest.shotgrid.autodesk.com',
                api_key=script_dict['eventTrigger'],
                script_name = 'eventTrigger',
            )


def allocate_nk_file( old_id ):
    vn_tag = {
            'id': 16936,
            'name': 'ww_vietnam',
            'type': 'Tag'
    }

    filters = [
        ['attribute_name','is','tags'],
        ['event_type','is','Shotgun_PublishedFile_Change'],
        ['entity.PublishedFile.tags', 'in', vn_tag ],
    ]

    keys = [
        'created_at','description', 'entity','project.Project.name',
        'entity.PublishedFile.code',
        'entity.PublishedFile.path_cache',
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

    print( filters )
    print( '-' * 50 )
    result = sg.find_one(
        'EventLogEntry', filters , keys
    )

    if not result:
        result = {}
        result['id'] = old_id
        print( 
            '[ Error ] There is no search result in EventLogEntity'
        )
        return old_id

    if old_id == result['id']:
        print( "[ Error ] Previous ID is same with old id" )

        result = {}
        result['id'] = old_id
        return old_id

    log = '[ {:20} ] {} {}'.format( 
                            'Published Data', result['id'] , 
                            result['entity']['name'] 
                    ) 
    print( log )

    if result['entity.PublishedFile.path_cache' ]:
        ftp_filepath = '/ftp/west_rnd/shotgrid_pub/show/' + result['entity.PublishedFile.path_cache' ]
        if not os.path.exists( ftp_filepath ):
            print(  
                    '[ {:20}] {} \n {}\n'.format(
                                    ' Error', ' File does not exist',
                                    ftp_filepath, 
                                    )
            )
            return result['id']
    if not DEV:
        tg_filepath = '/show/' + result['entity.PublishedFile.path_cache' ]
        p = Process( target = shutil.copyfile, args = ( ftp_filepath, tg_filepath, ) )
        p.start()
        p.join()

        print( 
            '[ {:20} ] {} '.format( 
                        'Allocated Nuke file', tg_filepath 
            )
        )
    return result['id']




def main( last_id = False ):
    return allocate_nk_file( last_id )




if __name__ == '__main__':
    main(  )
#    filters = [
#                    ['attribute_name', 'is', 'tags'], 
#                    ['event_type', 'is', 'Shotgun_PublishedFile_Change'], 
#                    ['entity.PublishedFile.tags', 'in', {'id': 16936, 'name': 'ww_vietnam', 'type': 'Tag'}], 
#                    ['id', 'greater_than', 98883865]
#                ]
#
#    keys = [
#        'created_at','description', 'entity','project.Project.name',
#        'entity.PublishedFile.code',
#        'entity.PublishedFile.path_cache',
#    ]
#    result = sg.find_one(
#        'EventLogEntry', filters , keys
#    )
#    pprint( result )

