require 'parquet'

module Parquet
  class Dataset
    def self.get(path)
      h = {}
      reader = Parquet::ArrowFileReader.new(path)
      table = reader.read_table
      table.each_column.collect do |col|
        h[col.name] = col.to_a
      end
      return h
    end
  end
end
