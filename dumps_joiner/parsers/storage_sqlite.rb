require 'sqlite3' #gem install sqlite3
require 'set'

class Storage
  def initialize(db_path)
    @db = SQLite3::Database.new(db_path)
    @db.results_as_hash = true
    @known_forum_ids = Set.new
    @count = {total: 0, saved: 0, created: 0, updated: 0, skipped: 0}
    @stt = Time.now

    @db.execute <<-SQL
      CREATE TABLE IF NOT EXISTS torrents (
        id INTEGER PRIMARY KEY,
        forum_id INTEGER,
        title TEXT,
        title_old TEXT,
        description TEXT,
        magnet TEXT,
        size INTEGER,
        created_at INTEGER,
        source TEXT
      )
    SQL

    @db.execute <<-SQL
      CREATE TABLE IF NOT EXISTS forums (
        id INTEGER PRIMARY KEY,
        category_id INTEGER,
        name TEXT
      )
    SQL

    @db.execute <<-SQL
      CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    SQL

    @db.execute "BEGIN"
  end

  def save(torrent)
    data = {
      id:          torrent[:id],
      forum_id:    torrent[:forum] && torrent[:forum][:id],
      title:       torrent[:title],
      title_old:   nil,
      description: torrent[:description],
      magnet:      torrent[:magnet],
      size:        torrent[:size],
      created_at:  torrent[:created_at].to_i,
      source:      torrent[:source],
    }

    row = @db.get_first_row "SELECT * FROM torrents WHERE id = ?", torrent[:id]
    if row
      data.delete_if{|k,v| v.nil? }
      cmp_keys = data.keys - [:source]
      if data.values_at(*cmp_keys) != row.values_at(*cmp_keys.map(&:to_s))
        data[:title_old] = row["title"] if data[:title] != row["title"]
        update(data)
      else
        @count[:skipped] += 1
      end
    else
      create(data)
    end
    
    create_forum(torrent[:forum]) if torrent[:forum]
    
    after_save
  end
  
  def update_description(id, description)
    @db.execute "UPDATE torrents SET description = ? WHERE id = ?", [description, id]
    after_save
  end
  
  def save_category(cat)
    begin
      @db.execute "INSERT INTO categories (id, name) VALUES (?, ?)", [cat[:id], cat[:name]]
    rescue SQLite3::ConstraintException => e
    end
  end
  
  def stop
    @db.execute "COMMIT"
  end
  
  private
  
  def create_forum(forum)
    return if @known_forum_ids.include? forum[:id]
    @known_forum_ids.add(forum[:id])
    begin
      @db.execute "INSERT INTO forums (id, category_id, name) VALUES (?, ?, ?)",
                  [forum[:id], forum[:category_id], forum[:name]]
    rescue SQLite3::ConstraintException => e
    end
  end
  
  def create(data)
    begin
      @db.execute "INSERT INTO torrents (#{ data.keys.join(',') }) VALUES (#{ (['?']*data.size).join(',') })", data.values
      @count[:saved] += 1
      @count[:created] += 1
    rescue SQLite3::ConstraintException => e
    end
  end
  
  def update(data)
    @db.execute "UPDATE torrents SET #{ data.keys.map{|k| "#{k}=?" }.join(',') } WHERE id=?", data.values + [data[:id]]
    @count[:saved] += 1
    @count[:updated] += 1
  end
  
  def after_save
    @count[:total] += 1
    print "s:#{@count[:saved]} c:#{@count[:created]} u:#{@count[:updated]} k:#{@count[:skipped]} t:#{@count[:total]} #{(@count[:total] / (Time.now-@stt)).to_i}t/s\r" if @count[:total] % 100 == 0
    if @count[:total] % 10000 == 0
      @db.execute "COMMIT"
      @db.execute "BEGIN"
    end
  end
end
