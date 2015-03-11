#!/usr/bin/env ruby
# encoding: UTF-8
#
# Tool for running java program, class files
# will be built automatically if necessary
#
# Usage:
#  > ruby run.rb [parameters_passed_to_java_program]
#

require 'find'
require 'fileutils'

THIS_FILE_DIR = File.dirname(File.expand_path(__FILE__))
CURRENT_DIR   = Dir.pwd

# basic directories
SRC_DIR = 'src'
LIB_DIR = 'lib'
BIN_DIR = 'bin'

# platform related constances
CLASSPATH_JOIN  = Gem.win_platform? ? ';' : ':'

# main class related constances
MAIN_CLASS_NAME = File.basename(THIS_FILE_DIR)
MAIN_CLASS_FILE = "#{BIN_DIR}/main/#{MAIN_CLASS_NAME}.class"
MAIN_CLASS      = "main.#{MAIN_CLASS_NAME}"

def exec_cmd(cmd)
    # open the following comment if commands are needed
    # puts cmd
    raise cmd if !system cmd
end

def get_files(dir, type)
    files = []

    if File.exist?(dir)
        Find.find(dir) { |file|
            files << file if file =~ /\.#{type}$/i
        }
    end

    return files
end

def newest_modified_time_of(files)
    newest_modified_time = 0

    files.each { |file|
        mtime = File.mtime(file).to_i
        newest_modified_time = mtime if mtime > newest_modified_time
    }

    return newest_modified_time
end

def class_files_out_of_date(source_files, class_files)
    newest_modified_time_of(source_files) > newest_modified_time_of(class_files)
end

def main_class_file_not_found()
    !File.exists?(MAIN_CLASS_FILE)
end

def run_java(args)
    # get files
    source_files = get_files(SRC_DIR, 'java')
    jar_files    = get_files(LIB_DIR, 'jar')
    class_files  = get_files(BIN_DIR, 'class')

    # classpath
    classpath_for_compile = (jar_files + Array(SRC_DIR)).join(CLASSPATH_JOIN)
    classpath_for_run     = (jar_files + Array(BIN_DIR)).join(CLASSPATH_JOIN)

    # build class files if necessary
    if main_class_file_not_found || class_files_out_of_date(source_files, class_files)
        FileUtils.mkpath BIN_DIR

        exec_cmd "javac -encoding UTF-8 -sourcepath #{SRC_DIR} -d #{BIN_DIR} -classpath #{classpath_for_compile} #{source_files.join(' ')}"
    end

    # run java program
    exec_cmd "java -classpath #{classpath_for_run} #{MAIN_CLASS} #{args}"
end

def main(argv)
    Dir.chdir THIS_FILE_DIR
    run_java(argv.map { |x| x.index(/\s/) ? "\"#{x}\"" : x }.join(' '))
    Dir.chdir CURRENT_DIR
end

# run main
main(ARGV)
