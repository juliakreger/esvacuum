require 'elasticsearch'
require 'json'
require 'time'
require 'esvacuum/modifydocuments'
# The main call for es-vacuum
module Esvacuum

  # This is intended to forklift one Elasticsearch server's data to another Elasticsearch server.
  #
  # @example Standard Usage
  #
  #     esvacuum.execute source: 'localhost:9200', destination: 'remotehost:9200'
  #
  # @option arguments [String] :source A Hostname, Hostname and Port, or URL for a source ES Server.
  # @option arguments [String] :destination A Hostname, Hostname and Port, or URL for a target ES Server.
  #                                         If the name begins with a period or slash, the output is written to a file.
  # @option arguments [Number] :size A chunk size in which to drive the operation for tuning effiency.
  #                                  (default: 100)
  # @option arguments [Boolean] :verbose Verbose output.  (default: false)
  # @option arguments [String] :newindexname A string if defined that is used to replace the target document index.
  # @option arguments [String] :newtypename A string if defined that is used to replace the target document type.
  # @option arguments [String] :sourceindex A string defining a single index to read from if it exists.
  #

  def self.execute( arguments={} )
    if arguments[:source].nil?
      raise "Requires a source to be defined"
    end
    if arguments[:destination].nil?
      raise "Requires a destination to be defined"
    end
    if arguments[:size].nil?
      arguments[:size] = 100
    end
    if arguments[:verbose].nil?
      arguments[:verbose] = false 
    end
    if arguments[:destination] =~ /^[\/|.\/]/
      arguments[:outputfile] = true
    else
      arguments[:outputfile] = false
    end

    if arguments[:source] =~ /^[\/|.\/]/
      arguments[:sourcefile] = true
    else
      arguments[:sourcefile] = false
    end
    if arguments[:sourcefile] == false
      begin
        sourceClient = Elasticsearch::Client.new host: arguments[:source]
      rescue 
        raise "Error connecting to #{arguments[:source]}"
      end
    else
      if File.exists?(arguments[:source]) == true
        inputFile = File.open(arguments[:source],"rt")
      else
        raise("Error: File does not exist.")
      end    
    end

    if arguments[:outputfile] == false
      begin
        targetClient = Elasticsearch::Client.new host: arguments[:destination]
      rescue
        raise "Error opening #{arguments[:destination]}"
      end
    else
      if File.exists?(arguments[:destination]) == false
        outputFile = File.open(arguments[:destination],"wt")
      else
        raise("Error: File exists")
      end
    end

    if (( arguments[:sourcefile] == true ) && ( arguments[:outputfile] == true ))
      raise "Error: Both input and output from files is not permitted."
    end
    if arguments[:sourcefile] == true 
      raise "Error: Transformation not available when reading from a file." if !arguments[:newindexname].nil?
      raise "Error: Transformation not available when reading from a file." if !arguments[:newtypename].nil?
      raise "Error: Reading from file only supports complete bulk loading." if !arguments[:sourceindex].nil?
    end

    if arguments[:sourcefile] == false
      if !arguments[:sourceindex].nil?
        indexqueryresponse = Hash.new
        indexqueryresponse = { arguments[:sourceindex] => "" }
      else
        indexqueryresponse = sourceClient.indices.get_mapping
      end
      indexqueryresponse.each_key do | indexname |
        beginning_time = Time.now
        begin
          sourceSearchResponse = sourceClient.search index: indexname,
                                                 size: arguments[:size].to_i,
                                                 scroll: '5m',
                                                 search_type: 'scan',
                                                 body: { query: { match_all: {} } }
        rescue
          raise "Error Searching Index: #{indexname}"
          next
        end

        puts "Processing Index: #{indexname}" if arguments[:verbose] == true

        recordcount = 1
        sourceSearchResponse = sourceClient.scroll scroll: '5m', scroll_id: sourceSearchResponse['_scroll_id']

        puts "Index #{indexname} contains #{sourceSearchResponse['hits']['total']} items." if arguments[:verbose] == true

        until sourceSearchResponse['hits']['hits'].size == 0 do

          dataBlock = Array.new
          records = sourceSearchResponse['hits']['hits']

          dataBlock = Esvacuum::Modifydocuments.execute arguments, records
          
          recordcount += dataBlock.size
          if arguments[:outputfile] == false
            targetClient.bulk body: dataBlock,
                               consistency: "one",
                               refresh: false
          else
            outputFile << Elasticsearch::API::Utils.__bulkify(dataBlock)
          end
          if arguments[:verbose] == true
            end_time = Time.now
            puts "Records #{(recordcount - 1)} completed in #{(end_time - beginning_time).round(2)} seconds"
          end

          sourceSearchResponse = sourceClient.scroll scroll: '5m', scroll_id: sourceSearchResponse['_scroll_id']
        end

        if arguments[:verbose] == true
          rps = (sourceSearchResponse['hits']['total'] / (end_time - beginning_time)).round(3)
          puts "Completed Index #{indexname} in #{(Time.now - beginning_time)} - #{rps} records/second"
        end

      end
    else

      beginning_time = Time.now
      recordCount = 0
      chunksize = ( 2 * arguments[:size].to_i )
      chunk = 1
      dataArray = Array.new

      inputFile.each do | line |
        if ( chunk.eql?(chunksize) || inputFile.eof? )
          dataBlock = dataArray.join("\n")
          targetClient.bulk body: dataBlock,
                            consistency: "one",
                            refresh: false
          recordCount += ( chunk / 2)
          puts "Records #{recordCount} completed in #{(Time.now - beginning_time).round(2)} seconds" if arguments[:verbose] == true
          dataArray = Array.new
          chunk = 0 
        end
        dataArray << line
        chunk += 1
      end

      if arguments[:verbose] == true
        rps = (recordCount / (end_time - beginning_time)).round(3)
        puts "Completed in #{(Time.now - beginning_time)} - #{rps} records/second"
      end
      inputFile.close
    end
    if arguments[:outputfile] == true
      outputFile.close
    end
    puts "Completed" if arguments[:verbose] == true
    
  end
end
