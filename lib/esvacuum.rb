require 'elasticsearch'
require 'json'
require 'time'

# The main call for es-vacuum
class Esvacuum
  # This is intended to forklift one Elasticsearch server's data to another Elasticsearch server.
  #
  # Example:
  #
  def self.execute( source = "localhost:9200", target = "localhost:9201", chunksize = 100 )
    # TODO
    #  
    #
    begin
      sourceClient = Elasticsearch::Client.new host: source
    rescue 
      puts "Error connecting to #{source}"
    end
    begin
      targetClient = Elasticsearch::Client.new host: target
    rescue 
      puts "Error connecting to #{target}"  
    end
    indexqueryresponse = sourceClient.indices.get_mapping
    indexqueryresponse.each_key do | indexname |
      beginning_time = Time.now
      begin
        sourceSearchResponse = sourceClient.search index: indexname,
                                                 size: chunksize,
                                                 scroll: '5m',
                                                 search_type: 'scan',
                                                 body: { query: { match_all: {} } }
      rescue
        puts "Error Searching Index: #{indexname}"
        next
      end
      puts "Processing Index: #{indexname}"
      recordcount = 1
      sourceSearchResponse = sourceClient.scroll scroll: '5m', scroll_id: sourceSearchResponse['_scroll_id']
      puts "Index #{indexname} contains #{sourceSearchResponse['hits']['total']} items."
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
        end_time = Time.now
        puts "Records #{(recordcount - 1)} completed in #{(end_time - beginning_time).round(2)} seconds"
        sourceSearchResponse = sourceClient.scroll scroll: '5m', scroll_id: sourceSearchResponse['_scroll_id']
      end
      end_time = Time.now
      rps = (sourceSearchResponse['hits']['total'] / (end_time - beginning_time)).round(3)
      puts "Completed Index #{indexname} in #{(end_time - beginning_time)} - #{rps} records/second"
    end
    puts "Completed"
  end
end
