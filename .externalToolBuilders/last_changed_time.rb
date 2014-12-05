#!/usr/bin/env ruby
# encoding: UTF-8
#

require 'open3'
require 'nokogiri'
require 'time'

# get last changed time from https://github.com/xxjapp/HBShell
def last_changed_time()
    tmp_file = '/tmp/HBShell.html'

    # use the following instead of "svn info --xml https://github.com/xxjapp/HBShell" because of
    #  - 'svn' may not exist
    #  - 'wget' is faster
    cmd = "wget --no-check-certificate --output-document=#{tmp_file} https://github.com/xxjapp/HBShell/commits/master"
    stdin, stdout, stderr = Open3.popen3(cmd)

    # wait for end
    stdout.readlines

    # parse page data
    xml  = Nokogiri::XML(IO.read(tmp_file))
    time = xml.xpath("//div[@class='commit-meta']/time/@datetime").to_s

    # return result
    begin
        return Time.parse(time).localtime.to_s
    rescue
        return "Not available"
    end
end

if __FILE__ == $0
    print last_changed_time()
end
