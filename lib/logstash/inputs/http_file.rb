# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket"
require "http"

class LogStash::Inputs::HttpFile < LogStash::Inputs::Base
  class Interrupted < StandardError; end
  config_name "http_file"
  default :codec, "plain"

  # The url to listen on.
  config :url, :validate => :string, :required => true
  # refresh interval
  config :interval, :validate => :number, :default => 10
  #start position 
  config :start_position, :validate => [ "beginning", "end"], :default => "end"

  def initialize(*args)
    super(*args)
  end # def initialize

  public
  def register
    @host = Socket.gethostname
    @logger.info("HTTP_FILE PLUGIN LOADED url=#{@url}")
  end

def run(queue)
	pattern = (@url.match(/\/?([^?\/]*)$/)[1]).gsub(/(\*(?!$))/,'.*?').gsub(/\*$/,'.*') #<file>.<extension>
    url = @url.gsub(/\/?([^?\/]*)$/,"") << "/"#http://<addrress:port>
    begin     
		@logger.info("Start http get files from url=#{url}")	
        response = HTTP.get(url)
      rescue Errno::ECONNREFUSED
        @logger.error("HTTP_FILE Error: Connection refused url=#{url}")
        sleep @interval
        retry
    end #end exception
    files = ((response.body).to_s).scan(/<A HREF=".*?">(.*?)<\/A>/).flatten 
    file_position = {}
    files.each do |file|
	@logger.info("Test pattern for file=#{file}")
      if file.match(/#{pattern}$/) 
        if @start_position == "beginning"
          file_position[file] = 0
        else          	
          begin
            response = HTTP.head(url+file);            
            file_position[file] = (response['Content-Length']).to_i
          rescue Errno::ECONNREFUSED
            @logger.error("Error: Connection refused to file: #{file}")
			next            
          end #end exception
        end #end if position
      end #end if match
    end#end each
    Stud.interval(@interval) do
      file_position.each do | file, position |
        begin
		 @logger.info("HTTP_FILE url=#{url+file}")
          response = HTTP.head(url+file); 
          new_position = (response['Content-Length']).to_i
		  @logger.info("HTTP_FILE url=#{url+file} file_size=#{position} new_file_size=#{new_position}")
          next if new_position == position # file not modified
          file_position[file] = 0 if new_position < position # file truncated => log rotation
          response = HTTP[:Range => "bytes=#{position}-#{new_position}"].get(url+file)
          if (200..226) === (response.code).to_i
            position += (response['Content-Length']).to_i
            file_position[file] = position
            messages = ((response.body).to_s).lstrip
            messages.each_line do | message |
            message = message.chomp
              if message != ''
                event = LogStash::Event.new("message" => message, "host" => @host)
                decorate(event)
                queue << event
              end #end if empty message
            end # end do
          end #end if code
        rescue Errno::ECONNREFUSED
          @logger.error("Error: Connection refused")
          next
        end #end exception               
      end#end each
    end#end loop
  end #end run
end #class
