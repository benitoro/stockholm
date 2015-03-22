from stockholm import Stockholm
import option
import os

def checkFoldPermission(path):
    if(path == 'USER_HOME/tmp/stockholm_export'):
        path = os.path.expanduser('~') + '/tmp/stockholm_export'
    try:
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            txt = open(path + os.sep + "test.txt","w")
            txt.write("test")
            txt.close()
            os.remove(path + os.sep + "test.txt")
            
    except Exception as e:
        print(e)
        return False
    return True

def main():
    args = option.parser.parse_args()
    if not checkFoldPermission(args.store_path):
        print('\nPermission denied: %s' % args.store_path)
        print('Please make sure you have the permission to save the data!\n')
    else:
        print('Stockholm is starting...\n')
        stockh = Stockholm(args)
        stockh.run()
        print('Stockholm is done...\n')

if __name__ == '__main__':
    main()
