#!/usr/bin/python

# This script is used for managing S3 buckets on AWS.


#############################################################
#      CONFIGURATION AREA, SET YOUR API KEY HERE            #
# ----------------------------------------------------------# 
AWS_ACCESS_KEY = 'XXXXXXXXXXXXXXXXXXXXX'                     
AWS_SECRET_KEY  = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
DEFAULT_BUCKET_NAME = 'test'
DEFAULT_SUBDIR_OPTION = 'no'
DEFAULT_NUMBER_DUMPS = 7 
#############################################################

import boto
import commands
import os 
import hashlib
import md5

from os.path import abspath, split, basename
from os.path import exists, isfile, expanduser
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
from boto.exception import S3CreateError

READ_CHUNK = 8192

conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)


def ensure_the_library_is_installed(name):
    print 'You have to install the python library "%s" in order to use ' \
          'this script' % name
    print
    print 'Just type: sudo pip install %s and you should be fine :)' % name
    raise SystemExit(1)

try:
    from argh import *
except ImportError:
    ensure_the_library_is_installed('argh')

@arg('--bucket', default=DEFAULT_BUCKET_NAME,
    help='The name of the S3 bucket to create',
)

def create_bucket(args):
    bucket = conn.create_bucket(args.bucket)

@arg('--bucket', 
    help='The name of the S3 bucket to delete',
)

def delete_bucket(args):
    "Deletes an S3 bucket"
    bucket = conn.get_bucket(args.bucket)
    for key in bucket.list():
       bucket.delete_key(key) 
    bucket = conn.delete_bucket(args.bucket)

@arg('--bucket', default=DEFAULT_BUCKET_NAME,
    help='The name of the S3 bucket that will hold your file',
)

@arg('--dump',
    help='The dump to upload to S3',
)

def storedump(args):
    "Stores MySQL dumps to an S3 bucket"
    x = 0
    tmp = []
    fullpath = expanduser(args.dump)
    if not exists(fullpath):
        print 'The directory "%s" does not exist!' % fullpath
        raise SystemExit(1)
    
    myfile = args.dump
    bucket = conn.get_bucket(args.bucket)
    # check if we have more than DEFAULT_NUMBER_DUMPS
    for i in sorted(bucket.list(), key=lambda k: k.last_modified):
        x = x+1
        tmp.append(i.name)
    if x >= DEFAULT_NUMBER_DUMPS:
        print "Limit reached!"
        delfile = tmp[0]
        k = Key(bucket)
        k.key = delfile
        bucket.delete_key(k)
    k = Key(bucket)
    k.key = myfile
    k.set_contents_from_filename(myfile)

@arg('--bucket', default=DEFAULT_BUCKET_NAME,
    help='The name of the S3 bucket where the file is going to be deleted',
)

@arg('--file',
    help='The file to delete from an S3 bucket',
)


def delete_file(args):
    "Deletes a particular file from an S3 bucket"
    myfile = args.file
    bucket = conn.get_bucket(DEFAULT_BUCKET_NAME)
    k = Key(bucket)
    k.key = myfile
    bucket.delete_key(k)

@arg('--bucket', default=DEFAULT_BUCKET_NAME,
    help='The name of the S3 bucket that will hold your file',
)

def listfiles(args):
    a = []
    bucket = conn.get_bucket(args.bucket)
    for key in bucket.list():
        a.append(key)
    a = sorted(a, key=lambda k: k.last_modified)
    for key in a:
        print "%s, %s \n" % (key.name, key.last_modified)

@arg('--bucket', default=DEFAULT_BUCKET_NAME,
    help='The name of the S3 bucket that will hold your file',
)

@arg('--path',
    help='The path of the file or directory to upload to S3',
)

@arg('--subdironly', default=DEFAULT_SUBDIR_OPTION,
    help='Only uploads subdirectories to the bucket',
)


def loadassets(args):
    "Uploads a particular file or set of files to an S3 bucket"
    z = []
    fullpath = abspath(expanduser(args.path))
    print "the fullpath is %s" % fullpath
    if not exists(fullpath):
        print 'The directory "%s" does not exist!' % fullpath
        raise SystemExit(1) 
    try:
        #bucket = conn.get_bucket(args.bucket)
        bucket = conn.create_bucket(args.bucket)
        print "bucket exists!" 
    except S3ResponseError:
        print "bucket doesnt exist, creating it.."  
        bucket = conn.create_bucket(args.bucket)
    except S3CreateError:
        print "bucket is not available, choose another name" 
        raise SystemExit(1)
    if not isfile(fullpath):
        cmd = 'find "%s" -type f' % fullpath
        result = commands.getoutput(cmd)
        array = result.split()
        for i in array:
            if args.subdironly == "yes":
                print "Subdirectories enabled..."
                z = i[ len(fullpath)+1:]
            else:
                print "Subdirectories disabled..."
                y = len(fullpath)-fullpath.rfind("/")
                z = i[len(fullpath)-y:]
            bucket = conn.get_bucket(args.bucket)
            k = Key(bucket)
            k.key = z
            print "Uploading %s" % k.key
        #    print "Uploading %s" % i 
            k.set_contents_from_filename(i)
        print "Upload of assets finished!"
    else:
        bucket = conn.get_bucket(args.bucket)
        k = Key(bucket)
        k.key = fullpath
        print "Uploading %s" % args.bucket
        k.set_contents_from_filename(fullpath)
        print "Upload of assets finished!"

def list_buckets(args):
    buckets = conn.get_all_buckets()
    for i in buckets:
        print "%s" % i

@arg('--bucket', default=DEFAULT_BUCKET_NAME,
    help='The name of the S3 bucket that will hold your file',
)

@arg('--path',
    help='The path of the file or directory to upload to S3',
)


def putonly(args):
    fullpath = abspath(expanduser(args.path))
    print "the fullpath is %s" % fullpath
    if not exists(fullpath):
        print 'The directory "%s" does not exist!' % fullpath
        raise SystemExit(1)
    try:
        bucket = conn.get_bucket(args.bucket)
        #bucket = conn.create_bucket(args.bucket)
        print "bucket exists!"
    except S3ResponseError:
        print "bucket doesnt exist,  exiting.."
        raise SystemExit(1)
        #bucket = conn.create_bucket(args.bucket)
    z = [] 
    k = Key(bucket)    
    cmd = 'find "%s" -type f' % fullpath
    result = commands.getoutput(cmd)
    array = result.split()
    for i in array:
        if args.subdironly == "yes":
            print "Subdirectories enabled..."
            z = i[ len(fullpath)+1:]
        else:
            print "Subdirectories disabled..."
            y = len(fullpath)-fullpath.rfind("/")
            z = i[len(fullpath)-y:]

        k = bucket.get_key(z)
        etag = k.etag[1:-1]
        fp = open(i, 'rb')
        hash = hashlib.md5()
        data = fp.read(READ_CHUNK)
        while data:
            hash.update(data)
            data = fp.read(READ_CHUNK)
        fp.close()
        digest = hash.hexdigest()
        if etag != digest:
             print "Uploading %s" % i 
             k.set_contents_from_filename(i)
             print "Upload of assets finished!"
        else:
            print "File %s is not new, canceling uploading!" % i
    print "Operation finished..."
    
if __name__ == '__main__':
    p = ArghParser()
    p.add_commands([create_bucket, delete_bucket, storedump, delete_file, listfiles, loadassets, list_buckets, putonly])
    p.dispatch()

