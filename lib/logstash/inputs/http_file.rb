# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket"
require "http"
require "base64"
require "stringio"

class LogStash::Inputs::HttpFile < LogStash::Inputs::Base
  class Interrupted < StandardError; end
  config_name "http_file"
  default :codec, "plain"

  config :url, :validate => :string, :required => true
  config :interval, :validate => :number, :default => 5
  config :start_position, :validate => ["beginning", "end"], :default => "end"
  config :username, :validate => :string, :default => ""
  config :password, :validate => :string, :default => ""
  config :max_request_bytes, :validate => :number, :default => 1048576
  config :sincedb_path, :validate => :string  

  def initialize(*args)
    super(*args)
  end

  public
  def register
    @host = Socket.gethostname
    if @sincedb_path.nil?
      if ENV["SINCEDB_DIR"].nil? && ENV["HOME"].nil?
        @logger.error("No SINCEDB_DIR or HOME environment variable set, I don't know where " \
                      "to keep track of the files I'm watching. Either set " \
                      "HOME or SINCEDB_DIR in your environment, or set sincedb_path in " \
                      "in your Logstash config for the file input with " \
                      "path '#{@path.inspect}'")
        raise # TODO(sissel): HOW DO I FAIL PROPERLY YO
      end
      #pick SINCEDB_DIR if available, otherwise use HOME
      sincedb_dir = ENV["SINCEDB_DIR"] || ENV["HOME"]
      # Join by ',' to make it easy for folks to know their own sincedb
      # generated path (vs, say, inspecting the @path array)
      @sincedb_path = File.join(sincedb_dir, ".sincedb_" + Digest::MD5.hexdigest(@url))
    end
    if File.directory?(@sincedb_path)
      raise ArgumentError.new("The \"sincedb_path\" argument must point to a file, received a directory: \"#{@sincedb_path}\"")
    end

    # Get position from file
    @auth_string = ""
    if @username != ''
      @auth_string = Base64.strict_encode64("#{@username}:#{@password}") 
    end
   
    response = HTTP[:Authorization => "Basic #{@auth_string}"].head(@url);
    if !((200..226).to_a.include? response.code.to_i) 
      raise ArgumentError.new("HTTP_FILE \"#{@url}\" return #{response.code}")
    end
    
    if @start_position == "beginning"
      @last_position = File.open(@sincedb_path, File::RDWR|File::CREAT, 0644).read.to_i
    else
      @last_position = response['Content-Length'].to_i
    end
    @logger.info("HTTP_FILE PLUGIN LOADED url=\"#{@url}\" last_position=#{@last_position}")
  end

  def run(queue)    
    Stud.interval(@interval) do
      begin
        response = HTTP[:Authorization => "Basic #{@auth_string}"].head(@url);        
        new_file_size = response['Content-Length'].to_i

        next if new_file_size == @last_position # file not modified. Skip
        @last_position = 0 if new_file_size < @last_position # file truncated => log rotation  

        skip_last_line = false
        delta_size = new_file_size - @last_position
        if delta_size > @max_request_bytes
            new_file_size = @last_position + @max_request_bytes
            skip_last_line = true
        end
        response = HTTP[:Range => "bytes=#{@last_position}-#{new_file_size}", :Authorization => "Basic #{@auth_string}"].get(@url)
        if (200..226).to_a.include? response.code.to_i
          messages = StringIO.new(response.body.to_s)
          messages.each_line do | message |
            if skip_last_line && messages.eof?  
               @last_position -= message.bytesize
               break
            end
            message = message.chomp
            if message != ''
              event = LogStash::Event.new("message" => message, "host" => @host)
              decorate(event)
              queue << event
            end
          end
          @last_position += response['Content-Length'].to_i
          File.open(@sincedb_path, File::RDWR|File::CREAT, 0644) { | f | f.write(@last_position.to_s) }
        else
          @logger.error("HTTP_FILE \"#{@url}\" return #{response.code}")
        end 
      rescue Errno::ECONNREFUSED
        @logger.error("HTTP_FILE Error: Connection refused url=#{@url}")
        sleep @interval
        retry
      end 
    end 
  end 
end 
