#!/usr/bin/env ruby
# encoding: UTF-8
#

require 'open3'
require 'time'

def get_info(url, pattern)
    cmd = "curl --insecure #{url}"
    # puts cmd

    Open3.popen3(cmd) { |stdin, stdout, stderr|
        return stdout.read.match(pattern)[1]
    }
end

# get last changed time from github api
def last_changed_time()
    url_of_latest_commit = get_info('https://api.github.com/repos/xxjapp/HBShell/git/refs/heads', /"url": "(.*commits.*)"/)
    last_changed_time    = get_info(url_of_latest_commit, /"date": "(.*)"/)

    begin
        return Time.parse(last_changed_time).localtime.to_s
    rescue
        return 'Not available'
    end
end

if __FILE__ == $0
    print last_changed_time()
end
