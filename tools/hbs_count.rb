#!/usr/bin/env ruby
# encoding: utf-8
#
# Introduction:
#   count table, row, family, qualifier in a hbs file
#
# Usage:
#   > ruby hbs_count.rb <hbs_file>
#

################################################################
# module HbsCounter

module HbsCounter
    def self.count file
        result  = {nTable: 0, nRow: 0, nFamily:0, nQualifier: 0, hasMoreQualifier: false, tables: {}}
        t       = nil
        r       = nil
        f       = nil
        status  = :ready

        handle_table = -> {
            t = $1
            result[:tables][t] = {nRow: 0, nFamily:0, nQualifier: 0, hasMoreQualifier: false, rows: {}}
            result[:nTable] += 1
            status = :table_found
        }

        handle_row = -> {
            r = $1
            result[:tables][t][:rows][r] = {nFamily: 0, nQualifier: 0, hasMoreQualifier: false, families: {}}
            result[:tables][t][:nRow] += 1
            result[:nRow] += 1
            status = :row_found
        }

        handle_family = -> {
            f = $1
            result[:tables][t][:rows][r][:families][f] = {nQualifier: 0, hasMoreQualifier: false}
            result[:tables][t][:rows][r][:nFamily] += 1
            result[:tables][t][:nFamily] += 1
            result[:nFamily] += 1
            status = :family_found
        }

        handle_qualifier = -> {
            result[:tables][t][:rows][r][:families][f][:nQualifier] += 1
            result[:tables][t][:rows][r][:nQualifier] += 1
            result[:tables][t][:nQualifier] += 1
            result[:nQualifier] += 1
            status = :qualifier_found
        }

        handle_qualifier_omit = -> {
            result[:tables][t][:rows][r][:families][f][:nQualifier] += 1
            result[:tables][t][:rows][r][:nQualifier] += 1
            result[:tables][t][:nQualifier] += 1
            result[:nQualifier] += 1
            result[:tables][t][:rows][r][:families][f][:hasMoreQualifier] = true
            result[:tables][t][:rows][r][:hasMoreQualifier] = true
            result[:tables][t][:hasMoreQualifier] = true
            result[:hasMoreQualifier] = true
            status = :qualifier_found
        }

        File.open file, 'r:utf-8' do |fobj|
            fobj.each do |line|
                line.chomp!
                break if line.empty?

                case status
                when :ready
                    if line =~ /^T: (.+)/
                        handle_table.call
                    end
                when :table_found
                    if line =~ /^ R: (.+)/
                        handle_row.call
                    else
                        raise line
                    end
                when :row_found
                    if line =~ /^  F: (.+)/
                        handle_family.call
                    else
                        raise line
                    end
                when :family_found
                    if line =~ /^   Q: (.+)/
                        handle_qualifier.call
                    else
                        raise line
                    end
                when :qualifier_found
                    if line =~ /^   Q: (.+)/
                        handle_qualifier.call
                    elsif line =~ /^  F: (.+)/
                        handle_family.call
                    elsif line =~ /^ R: (.+)/
                        handle_row.call
                    elsif line =~ /^T: (.+)/
                        handle_table.call
                    elsif line == '   ...'
                        handle_qualifier_omit.call
                    elsif line == '---------------------------------------'
                        break
                    else
                        raise line
                    end
                else
                    raise line
                end
            end
        end

        more_then = ->(hasMoreQualifier) {hasMoreQualifier ? "> " : ""}

        puts "total: (nTable: #{result[:nTable]}, nRow: #{result[:nRow]}, nFamily: #{result[:nFamily]}, nQualifier: #{more_then.call(result[:hasMoreQualifier])}#{result[:nQualifier]})"
        puts "-------------------------------"

        result[:tables].each { |t, tr|
            puts "T: #{t} (nRow: #{tr[:nRow]}, nFamily: #{tr[:nFamily]}, nQualifier: #{more_then.call(tr[:hasMoreQualifier])}#{tr[:nQualifier]})"

            tr[:rows].each { |r, rr|
                puts " R: #{r} (nFamily: #{rr[:nFamily]}, nQualifier: #{more_then.call(rr[:hasMoreQualifier])}#{rr[:nQualifier]})"

                rr[:families].each { |f, fr|
                    puts "  F: #{f} (nQualifier: #{more_then.call(fr[:hasMoreQualifier])}#{fr[:nQualifier]})"
                }
            }
        }

        puts "-------------------------------"
    end
end

################################################################
# main

if __FILE__ == $0
    HbsCounter.count ARGV[0]
end
