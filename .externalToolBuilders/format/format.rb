#!/usr/bin/env ruby
# encoding: UTF-8
#
# - format java source files using uncrustify
# - current directory will be used as argument processed if no arguments supplied
#
# Usage:
#   > ruby format.rb [directory or file list]
#
# Example:
#   > ruby format.rb
#   > ruby format.rb ./src
#   > ruby format.rb ./src tmp.java
#

require 'tmpdir'
require 'yaml'
require 'open3'

# ----------------------------------------------------------------
# initialize

THIS_FILE_DIR  = File.dirname(File.expand_path(__FILE__))
CURRENT_DIR    = Dir.pwd

def tmpfile(file)
    Dir.tmpdir + '/_ruby_tmp/' + file.sub(':', '').gsub('/', '_')
end

# source file modified-time cache file
MTIMES_YAML    = tmpfile(THIS_FILE_DIR + '/mtimes.yaml')

# if USE_SVN_STATUS = true, "svn status #{dir}" will be used
# if USE_SVN_STATUS = false, "svn list -R #{dir}" will be used
USE_SVN_STATUS = true

# uncrustify configure file for java
CONFIG_FILE    = THIS_FILE_DIR + '/uncrustify_java.cfg'

# See: http://rbjl.net/35-how-to-properly-check-for-your-ruby-interpreter-version-and-os
module OS
  class << self
    def is?(what)
      what === RbConfig::CONFIG['host_os']
    end
  end

  module_function

  def is_linux?
    OS.is? /linux|cygwin/
  end

  def is_mac?
    OS.is? /mac|darwin/
  end

  def is_windows?
    OS.is? /mswin|^win|mingw/
  end
end

def uncrustify()
    uncrustify = THIS_FILE_DIR + '/uncrustify'

    if OS.is_mac?
        uncrustify += '.mac'                    # Mac
    elsif OS.is_windows?
        uncrustify += '.exe'                    # Windows
    else
        uncrustify                              # Linux
    end
end

# uncrustify path
UNCRUSTIFY     = uncrustify()

# ----------------------------------------------------------------
# methods

def load_mtimes()
    mtimes = YAML.load_file(MTIMES_YAML)
    mtimes.is_a?(Hash) ? mtimes : {}
rescue
    {}
end

def save_mtimes(mtimes)
    FileUtils.mkdir_p(File.dirname(MTIMES_YAML))

    File.open(MTIMES_YAML, 'w') { |f|
        f.puts mtimes.to_yaml
    }
end

def get_files(argv)
    files = []
    dirs  = argv.length > 0 ? argv : ['.']

    dirs.each { |dir|
        if USE_SVN_STATUS
            files += get_files_by_svn_status(dir)
        else
            files += get_files_by_svn_list(dir)
        end
    }

    files
end

def get_files_by_svn_status(dir)
    files = []

    stdin, stdout, stderr = Open3.popen3("svn status #{dir}")
    stdout.readlines.each { |line|
        flag = line[0]
        file = line[8..-1].chomp

        files << file if (flag == 'M' || flag == 'A') && File.extname(file).casecmp('.java') == 0 && updated?(file)
    }

    files
end

def get_files_by_svn_list(dir)
    files = []

    stdin, stdout, stderr = Open3.popen3("svn list -R #{dir}")
    stdout.readlines.each { |line|
        file = "#{dir.gsub('\\', '/')}/#{line.chomp}"

        files << file if File.extname(file).casecmp('.java') == 0 && updated?(file)
    }

    files
end

def updated?(file)
    mtime0 = @mtimes[file].to_i
    mtime  = File.mtime(file).to_i

    mtime > mtime0
rescue Errno::ENOENT
    @mtimes.delete(file)
    false
end

def update_mtime(file)
    @mtimes[file] = File.mtime(file).to_i
end

def format(files)
    files.each { |file|
        format_file(file)
        update_mtime(file)
    }
end

def format_file(file)
    `#{UNCRUSTIFY} -l JAVA -c #{CONFIG_FILE} --no-backup #{file} -q`
end

# ----------------------------------------------------------------
# main entry

def main(argv)
    Dir.chdir THIS_FILE_DIR + '/../..'

    @mtimes = load_mtimes()
    files = get_files(argv)
    format(files)
    save_mtimes(@mtimes)

    Dir.chdir CURRENT_DIR
end

main(ARGV)
