# :coding: utf-8
#
#
# TODO
#1.log setup
#2.foler에서 플러그인 호출
#3.플러그인 실행
#


import os
import sys
import imp
import time
from pprint import pprint
import datetime as dt
import shotgun_api3 as sa
import os
import glob
import yaml
import traceback


MOD_DIR     = os.path.abspath( os.path.dirname( __file__ ) )
PLUGIN_PATH = os.path.join( MOD_DIR , 'plugins' )
LOG_DIR     = os.path.join( MOD_DIR , 'log'     )


DEV = 0

class PluginCollection:
    def __init__( self ):
        self.files = [ Plugin( x ) for x in glob.glob( PLUGIN_PATH + os.sep + '*.py' ) \
                       if basename(x)[0] not in '_'
                       ]
        print( 'Plugins Collection' )
        print( [ x.name for x in self.files if 'util' not in x.name ] )                        

    def __iter__(self):
        for x in self.files:
            yield x


class Plugin:
    def __init__( self, path ):
        self._path = path
        self.name = basename( path )
        self.load()
        self.id_file  = MOD_DIR + os.sep + './last_id'  + os.sep +  self.name + '.id'
        # self.log_file = LOG_DIR + os.sep + self.name + '.' + dt.datetime.strftime( dt.datetime.now() , '%Y%m' )+ '.log'

    def load(self):
        self.plugin = imp.load_source( self.name , self._path )

    def main(self, arg ):
        self.plugin = imp.load_source( self.name , self._path )
        return self.plugin.main(arg)

    def __str__(self):
        return str( self.plugin )

    def excution_status(self):
        with open( MOD_DIR + os.sep + './config.yml') as f:
            data = yaml.load( f, Loader = yaml.FullLoader )
        status = data['plugins'][self.name]['excution']
        return status

    def set_status_id( self , _id ):
        with open( self.id_file , 'w' ) as f:
            f.write( str(_id) )

    def get_status_id( self ):
        if not os.path.exists( self.id_file ):
            return False
        with open( self.id_file ) as f:
            result = f.read()
        return int(result)


def timelog():
    return dt.datetime.strftime( dt.datetime.now(), '%Y-%m-%d %H:%M:%S' )


def get_week_of_month( date ):
    first_day = date.replace(day=1)
    first_weekday = first_day.weekday()
    return (date.day + first_weekday - 1) // 7 + 1


def get_week_ranges( now_obj ):
    now = dt.datetime.strftime( now_obj, '%Y-%m-%d' )
    year, month, _ = map( int, now.split('-') )

    first_day = dt.datetime(year, month, 1)
    last_day = ( first_day.replace(month=month % 12 + 1, day=1) - dt.timedelta(days=1) ).day

    start_date = None
    end_date = None
    today_week = get_week_of_month( now_obj )

    for day_item in range( 1, last_day +1 ):
        current_date = dt.datetime( year, month, day_item )
        week_of_month = get_week_of_month( current_date )

        if week_of_month == today_week:
            if start_date is None:
                start_date = current_date
            end_date = current_date
    
    if start_date and end_date:
        start_date = start_date.strftime( '%Y%m%d' )
        end_date = end_date.strftime( '%Y%m%d' )
    
    return "{0}~{1}".format( start_date, end_date )


def this_time_log():
    now = dt.datetime.now()

    if not os.path.exists( LOG_DIR ):
        os.makedirs( LOG_DIR )

    today = dt.datetime.strftime( now, '%Y-%m-%d' )
    year, month, day = today.split( '-' )
    year_month_dir = os.path.join( LOG_DIR , '{0}{1}'.format(year, month) )

    if not os.path.exists( year_month_dir ):
        os.makedirs( year_month_dir )
    
    week = get_week_ranges( now )
    week_dir = os.path.join( year_month_dir, week )

    if not os.path.exists( week_dir ):
        os.makedirs( week_dir )
    
    log_file = os.path.join( week_dir, 'event.{0}{1}{2}.log'.format(year, month, day) )
    if not os.path.exists( log_file ):
        file = open( log_file, 'w' )
        file.close()

    return log_file


def main():
    log_path = this_time_log()
    start_time = timelog()
    log_date = start_time.split(' ')[0]
    
    so = open( log_path, 'a+')
    sys.stdout = so

    print( " *** Started main event logger *** " )

    pc = PluginCollection()
    print( '-'*50 )
    print( '[Started log]' , start_time )
    print( '-'*50 )
    print()
    sys.stdout.flush()

    while True:
        time.sleep( 1 )

        current_time = timelog()
        current_date = current_time.split(' ')[0]

        if log_date != current_date:
            log_date = current_date
            log_path = this_time_log()

            so.close()
            so = open( log_path, 'a+' )
            sys.stdout = so

            print( " *** {0} main event logger *** ".format(log_date) )
            print()
            sys.stdout.flush()

#        log_path = log_filepath()
#        so = open( log_path, 'a+')
#        sys.stdout = so

       # print time.strftime( '%Y-%m-%d %H:%M:%S', time.localtime() )
       # sys.stdout.flush()
       # continue

#        for plugin in pc:
#            last_id = plugin.get_status_id()
#            result  = plugin.main( last_id )
#            plugin.set_status_id( result )
#        continue

        #########################################################################
        ##  Excution part
        ##  Non-comment for excution part
        #########################################################################
        for plugin in pc:
            print( plugin.name )
            #print( '[{}] id: {}'.format( plugin.name, str( last_id ) ) )
            if plugin.excution_status():
                if DEV:
                    last_id = plugin.get_status_id()
                    result  = plugin.main( last_id )
                    print( '[ DEV {} ] id: {}'.format( plugin.name, str( result ) ) )
                    result = result + 1
                    plugin.set_status_id( result )
                    #continue

                else:
                    # last_id = plugin.get_status_id()
                    # result = plugin.main( last_id )
                    #continue       
                    try:
                        last_id = plugin.get_status_id()
                        result = plugin.main( last_id )
                    except sa.ProtocolError:
                        print( "Protocol Error" )
                        print( "Error: {}".format(str(traceback.format_exc())) )
                        result = last_id
                    except KeyboardInterrupt:
                        print( "[ KeyboardInterrput ] %s"% timelog() )
                        break
                    except sa.lib.httplib2.ServerNotFoundError:
                        print( "ServerNotFoundError" )
                        print( "Error: {}".format(str(traceback.format_exc())))
                        result = last_id
                        break
                    except ValueError as e:
                        if "invalid literal for int() with base 10:" in str(e) :
                            failed_value = str(e).split(":")[-1].strip()
                            print("Invalid value for integer conversion.")
                            print("Fail Value : '{0}'".format(failed_value))
                            break
                        else:
                            print("[ ValueError ]")
                            print( "Error: {}".format(str(traceback.format_exc())) )
                    except:
                        print( '^'*80 )
                        print( '[ {} ] {} : {} '.format( plugin.name, ' Unknown error', last_id ) )
                        print( "Error: {}".format(str(traceback.format_exc())) )
                        print( '^'*80 )
                        result = last_id + 1
                    finally:
                        if plugin.excution_status():
                            plugin.set_status_id( result )
                        sys.stdout.flush()
                print( '[ {} ] excution_status {} : {}'.format( plugin.name, last_id, plugin.excution_status() ) )
                sys.stdout.flush()
#            print( '[ '+plugin._name + ' ] excution_status : ' + plugin.excution_status(), last_id ) 
#

def basename( path ):
    return os.path.splitext( os.path.basename(path))[0]


if __name__ == '__main__':
    main()





