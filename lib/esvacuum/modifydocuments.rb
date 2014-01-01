module Esvacuum
  class Modifydocuments

    # This module modifies the documents for insertion into the new database.  Under normal conditions, nothing is actually really done aside from the data being re-assembled..
    #
    # @option arguments [Hash] Arugments from the main file of the module, please reference ../esvacuum.rb.
    # @option records [Array] Array of records being passed in.
    #



    def self.execute( arguments, records ) 

      arguments[:modifyindex] = false
      arguments[:modifyindex] = true if !arguments[:newindexname].nil?
      arguments[:modifyindex] = true if !arguments[:newtypename].nil?

      dataBlock = Array.new 
      if arguments[:modifyindex] == false
        records.each do | record |
          tempHash = Hash.new
          tempHash = { "index" => { "_index" => record["_index"], "_type" => record["_type"], "_id" => record["_id"], "data" => record["_source"] }} 
          dataBlock << tempHash
        end
      else
        records.each do | record |
          tempHash = Hash.new
            if !arguments[:newindexname].nil? and arguments[:newtypename].nil?        
              tempHash = { "index" => { "_index" => arguments[:newindexname], "_type" => record["_type"], "_id" => record["_id"], "data" => record["_source"] }}
            elsif arguments[:newindexname].nil? and !arguments[:newtypename].nil?
              tempHash = { "index" => { "_index" => record["_index"], "_type" => arguments[:newtypename], "_id" => record["_id"], "data" => record["_source"] }}
            elsif !arguments[:newindexname].nil? and !arguments[:newtypename].nil?
              tempHash = { "index" => { "_index" => arguments[:newindexname], "_type" => arguments[:newtypename], "_id" => record["_id"], "data" => record["_source"] }}
            else
              raise "Error: Entered condition where instructions are to modify index or type values in documents, however neither modified."
            end
          dataBlock << tempHash
        end
      end
      return dataBlock
    end
  end
end
