require 'elasticsearch'
require 'json'
require 'time'

# The main call for es-vacuum
class Esvacuum

  # This is intended to forklift one Elasticsearch server's data to another Elasticsearch server.
  #
  # @example Standard Usage
  #
  #     esvacuum.execute source: 'localhost:9200', destination: 'remotehost:9200'
  #
  # @option arguments [String] :source A Hostname, Hostname and Port, or URL for a source ES Server.
  # @option arguments [String] :destination A Hostname, Hostname and Port, or URL for a target ES Server.
  # @option arguments [Number] :size A chunk size in which to drive the operation for tuning effiency.
  #                                  (default: 100)
  # @option arguments [Boolean] :verbose Verbose output.  (default: false)
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
      #TODO change to false
      arguments[:verbose] = false 
    end

    begin
      sourceClient = Elasticsearch::Client.new host: arguments[:source]
    rescue 
      raise "Error connecting to #{arguments[:source]}"
    end
    begin
      targetClient = Elasticsearch::Client.new host: arguments[:destination]
    rescue 
      raise "Error connecting to #{arguments[:destination]}"  
    end

    indexqueryresponse = sourceClient.indices.get_mapping
    indexqueryresponse.each_key do | indexname |
      beginning_time = Time.now
      begin
        sourceSearchResponse = sourceClient.search index: indexname,
                                                 size: arguments[:size],
                                                 scroll: '5m',
                                                 search_type: 'scan',
                                                 body: { query: { match_all: {} } }
      rescue
        raise "Error Searching Index: #{indexname}"
        next
      end
      if arguments[:verbose] == true
        puts "Processing Index: #{indexname}"
      end
      recordcount = 1
      sourceSearchResponse = sourceClient.scroll scroll: '5m', scroll_id: sourceSearchResponse['_scroll_id']

      if arguments[:verbose] == true
        puts "Index #{indexname} contains #{sourceSearchResponse['hits']['total']} items."
      end

      until sourceSearchResponse['hits']['hits'].size == 0 do

        dataBlock = Array.new
        records = sourceSearchResponse['hits']['hits']

        records.each do | record |
          tempHash = Hash.new
          tempHash = { "index" => { "_index" => record["_index"], "_type" => record["_type"], "_id" => record["_id"], "data" => record["_source"] }}
          dataBlock << tempHash
          recordcount += 1
        end

        targetClient.bulk body: dataBlock,
                          consistency: "one",
                          refresh: false

        if arguments[:verbose] == true
          end_time = Time.now
          puts "Records #{(recordcount - 1)} completed in #{(end_time - beginning_time).round(2)} seconds"
        end

        sourceSearchResponse = sourceClient.scroll scroll: '5m', scroll_id: sourceSearchResponse['_scroll_id']
      end

      if arguments[:verbose] == true
        end_time = Time.now
        rps = (sourceSearchResponse['hits']['total'] / (end_time - beginning_time)).round(3)
        puts "Completed Index #{indexname} in #{(end_time - beginning_time)} - #{rps} records/second"
      end

    end

    if arguments[:verbose] == true
      puts "Completed"
    end 

  end
end
