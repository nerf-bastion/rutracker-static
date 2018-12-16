require 'nokogiri' #gem install nokogiri
#require 'xz' #ruby-xz
#require 'zip' #gem install rubyzip
require 'time'
require_relative 'storage'


class Parser < Nokogiri::XML::SAX::Document
  def initialize(storage, source)
    @storage = storage
    @source = source
  end

  def start_element(name, attrs = [])
    #p [name, attrs]
    @elem = name
    case name
    when "torrent"
      @torrent = {source: @source}
      attrs.each do |name, value|
        @torrent[name.to_sym] = case name
          when "id", "size" then value.to_i
          when "registred_at" then Time.parse(value+" +03:00")
          else value
        end
      end
    when "forum"
      @torrent[:forum] = {id: attrs.find{|n,v| n=="id" }[1].to_i}
    end
  end

  def characters(string)
  end

  def cdata_block(string)
    #p [@elem, string.size]
    case @elem
      when "title", "magnet" then @torrent[@elem.to_sym] = string
      when "content" then  @torrent[:description] = string
      when "forum" then @torrent[:forum][:name] = string
    end
  end

  def end_element(name)
    #p "/"+name
    case name
    when "torrent"
      @storage.save(@torrent)
      @torrent = nil
    end
  end
end


# usage:
#  7z e -so rutracker.org_db_xml/backup.20161015122203.7z | tail -n+2 | cat <(echo "<torrents>") <(cat -) | ruby rutracker.rb ~/Documents/data/torrents.db 20161015.xml
#  unzip -p rutracker.org_db_xml_upd1/backup.20161212182126.zip | tail -n+2 | cat <(echo "<torrents>") <(cat -) | ruby rutracker.rb ~/Documents/data/torrents.db 20161212.xml
#  unzip -p rutracker.org_db_xml_upd2/backup.20170208185701.zip | ruby rutracker.rb ~/Documents/data/torrents.db 20170208.xml

db_path = ARGV[0] or raise "db_path"
source = ARGV[1] or raise "source"

storage = Storage.new(db_path)

Nokogiri::XML::SAX::Parser.new(Parser.new(storage, source)).parse(STDIN)

storage.stop

print "\ndone\n"

=begin
class ReadPrefixer
  def initialize(prefix, stream)
    @prefix = prefix
    @stream = stream
    @prefix_sent = false
  end
  def read(number_of_bytes = nil, buf = '')
    if !@prefix_sent
      @prefix_sent = true
      @prefix + " "*38 + @stream.read(number_of_bytes - @prefix.size, buf)[38..-1]
    else
      @stream.read(number_of_bytes, buf)
    end
  end
  def close
    @stream.close
  end
end

Zip::File.open('rutracker.org_db_xml_upd2/backup.20170208185701.zip') do |zip_file|
  zip_file.each do |entry|
    puts "Extracting #{entry.name}"
    Nokogiri::XML::SAX::Parser.new(Parser.new("20170208.xml")).parse(entry.get_input_stream)
  end
end

Zip::File.open('rutracker.org_db_xml_upd1/backup.20161212182126.zip') do |zip_file|
  zip_file.each do |entry|
    puts "Extracting #{entry.name}"
    Nokogiri::XML::SAX::Parser.new(Parser.new("20161212.xml")).parse(ReadPrefixer.new('<torrents>', entry.get_input_stream))
  end
end
=end
