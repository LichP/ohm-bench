#!/usr/bin/env ruby1.9.1

require 'ohm'
require 'benchmark'

class Ohm::Model
  alias_method :add_to_indices_original, :add_to_indices
  alias_method :delete_from_indices_original, :delete_from_indices

  def add_to_indices_pipelined
    db.pipelined do
      indices.each do |att|
        next add_to_index(att) unless collection?(send(att))
        send(att).each { |value| add_to_index(att, value) }
      end
    end
  end

  def delete_from_indices_pipelined
    indices_key = key[:_indices].smembers
    db.pipelined do
      indices_key.each do |index|
        db.srem(index, id)
      end
    end
        
    key[:_indices].del
  end
end

module OhmBench

CLASS_NAMES = %w[OneIndex TwoIndices FourIndices EightIndices]
ATTR_NAMES  = [:zero, :one, :two, :three, :four, :five, :six, :seven, :eight]

# Create classes
CLASS_NAMES.each_with_index do |class_name, i|
  const_set(class_name, Class.new(Ohm::Model) do
    1.upto(2 ** i) do |j|
      attribute ATTR_NAMES[j]
      index ATTR_NAMES[j]
    end
  end)
end

# Benchmark work method
def self.do_benchmark_work(exp)
  i = 2 ** exp
  10000.times do |j|
    const_get(CLASS_NAMES[exp]).create(Hash[ATTR_NAMES[0..exp].zip(Array.new(exp, j % 7)).flatten])
  end
end

# Flush database
puts "Flushing database"
Ohm.flush

# Benchmark
puts "Commencing benchmark"

Benchmark.bmbm do |x|
  0.upto(3) do |exp|
    i = 2 ** exp

    x.report("#{i} indices: no pipelining") do
      OhmBench.do_benchmark_work(exp)
    end

    x.report("#{i} indices: add_to_indices pipelining") do
      class Ohm::Model
        alias_method :add_to_indices, :add_to_indices_pipelined
      end

      OhmBench.do_benchmark_work(exp)

      class Ohm::Model
        alias_method :add_to_indices, :add_to_indices_original
      end      
    end

#    x.report("#{i} indices: delete_from_indices pipelining") do
#      class Ohm::Model
#        alias_method :delete_from_indices, :delete_from_indices_pipelined
#      end
#
#      OhmBench.do_benchmark_work(exp)
#
#      class Ohm::Model
#        alias_method :delete_from_indices, :delete_from_indices_original
#      end      
#    end

  end
end

end
