#!/usr/bin/env ruby
# encoding: UTF-8
#
# Tool to create version info file
#
# Usage:
#  > ruby create_version.rb
#

require 'find'
require 'nokogiri'
require 'open3'
require 'time'

# ----------------------------------------------------------------
# initialize

THIS_FILE_DIR  = File.dirname(File.expand_path(__FILE__))
CURRENT_DIR    = Dir.pwd

VERSION_FILE_PATH = 'src/main/Version.java'

# ----------------------------------------------------------------
# utils

def version()
    svnversion = `svnversion -n`
    version_num = svnversion.to_i

    if version_num.to_s != svnversion
        version_num += 1
    end

    sprintf("%04d", version_num)
rescue
    "UNKNOWN(svnversion not found)"
end

def build_time()
    Time.now
end

# get last changed time from https://github.com/xxjapp/HBShell
def last_changed_time()
    tmp_file = '/tmp/HBShell.html'

    # use the following instead of "svn info --xml https://github.com/xxjapp/HBShell" because of
    #  - 'svn' may not exist
    #  - 'wget' is faster
    cmd = "wget --no-check-certificate --output-document=#{tmp_file} https://github.com/xxjapp/HBShell"
    stdin, stdout, stderr = Open3.popen3(cmd)

    # wait for end
    stdout.readlines

    # parse page data
    xml  = Nokogiri::XML(IO.read(tmp_file))
    time = xml.xpath("//div[@class='authorship']/time/@datetime").to_s

    # return result
    Time.parse(time).to_s
end

# ----------------------------------------------------------------
# methods

def need_to_create?()
    # not exists
    return true if !File.exists?(VERSION_FILE_PATH)

    # binary files not created
    binary_found = false

    Find.find('bin') { |file|
        if file =~/\.class$/i
            binary_found = true
            break
        end
    }

    return true if !binary_found

    # other source file is newer than version file
    version_file_mtime = File.mtime(VERSION_FILE_PATH).to_i

    Find.find('src') { |file|
        file_mtime = File.mtime(file).to_i
        return true if file_mtime > version_file_mtime
    }

    # no need to create
    return false
rescue Errno::ENOENT    # no 'bin' directory
    return true
end

def create_version_file()
    # no need to create version file, return
    return if !need_to_create?()

    open(VERSION_FILE_PATH, 'w') { |f|
        f.puts "// This java source file is auto-genarated by '#{File.basename(__FILE__)}' at '#{Time.now}'"
        f.puts "// The following infos can be referenced in other parts of this project"
        f.puts "// Do not edit this file"
        f.puts ""
        f.puts "package main;"
        f.puts ""
        f.puts "public final class Version {"
        f.puts "    public static final String REVISION          = \"#{version()}\";"
        f.puts "    public static final String BUILD_TIME        = \"#{build_time()}\";"
        f.puts "    public static final String LAST_CHANGED_TIME = \"#{last_changed_time()}\";"
        f.puts "}"
    }
end

# ----------------------------------------------------------------
# main entry

def main(argv)
    Dir.chdir THIS_FILE_DIR + '/..'
    create_version_file()
    Dir.chdir CURRENT_DIR
end

main(ARGV)
