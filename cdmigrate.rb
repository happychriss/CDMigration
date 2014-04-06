require "rubygems"
require 'active_record'
require 'tmpdir'
require 'fileutils'
require 'aws/s3'
require 'gpgr'

S3_SOURCE_PATH = "/media/qStorage/DataCLeanDesk/docstore/" # source for files to upload

SOURCE_PATH = "/home/cneuhaus/CleanDesk/CDServer/public/docstore/" ### source for original PDF files

#TARGET_PATH = "/home/development/Projects/CD2/CDServer/public/docstore/"
TARGET_PATH = "/media/qStorage/docstore_tmp/"

TAG_MAPPING =[0, 1, 2, 3, 4, 19, 6, 19, 8, 12, 10, 12, 12, 13, 19, 12, 19, 17, 18, 19, 19, 21, 22, 23]

#######################################
class AWS_S3
  GPG_EMAIL_ADDRESS='email'
  AWS_S3_ACCESS_KEY='access'
  AWS_S3_SECRET_KEY='secret'
  AWS_S3_BUCKET='bucket'
end

ActiveRecord::Base.configurations["source"] = {
    :adapter => 'mysql2',
    :database => 'CDServer_production',
    :username => 'user',
    :password => 'martin'
}

##### Local Server
#ActiveRecord::Base.configurations["target"] = {
#    :adapter => 'mysql2',
#    :database => 'CD2Server_development',
#    :username => 'root',
#    :password => 'martin',
#}

### QNAS Server
ActiveRecord::Base.configurations["target"] = {
    :adapter => 'mysql2',
    :database => 'CD2Server_production',
    :username => 'database',
    :password => 'user',
    :host => 'ip'
}


class SourceDB < ActiveRecord::Base
  self.abstract_class = true
  establish_connection "source"
end

class TargetDB < ActiveRecord::Base
  self.abstract_class = true
  establish_connection "target"
end


class Docs < SourceDB

  def get_page_count
    count = Docs.where("parent_id =#{self.id}").count
    count=1 if count==0
    return count
  end

  def get_pages
    Docs.where("parent_id =#{self.id} and page_no <>0")
  end

  def self.get_titles
    Docs.where("page_no = 0")
  end

  def get_taggings
    SourceTagging.where("taggable_id=#{self.id}")
  end

end

class Pages < TargetDB

  def self.new_page(document_id, spage, cover_id)
    tpage=Pages.new
    tpage.content=spage.content
    tpage.created_at=spage.created_at
    tpage.document_id=document_id
    tpage.folder_id=spage.folder_id
    tpage.original_filename=spage.did.to_s+".pdf"
    tpage.position=spage.page_no
    tpage.fid=spage.fid
    tpage.cover_id=cover_id
    tpage.source=99 #migration
    tpage.status=2 # pages was processed by worker (content added)
    tpage.save!

    begin
      FileUtils.cp(SOURCE_PATH+spage.did.to_s+".pdf", TARGET_PATH+tpage.id.to_s+".pdf")
      FileUtils.cp(SOURCE_PATH+spage.did.to_s+".jpg", TARGET_PATH+tpage.id.to_s+".jpg")
#    FileUtils.cp(SOURCE_PATH+spage.did.to_s+"s.jpg", TARGET_PATH+tpage.id.to_s+"_s.jpg")
      res=%x[convert '#{SOURCE_PATH+spage.did.to_s+".jpg"}' -resize 350x490\! jpg:'#{TARGET_PATH+tpage.id.to_s+"_s.jpg"}']
    rescue
      puts "************************************ ERROR #{spage.did.to_s} *******************************"
    end
    return tpage.id

  end

end

class Document< TargetDB
end

class Cover < TargetDB

end

class SourceTagging < SourceDB
  self.table_name= "taggings"
end

class TargetTagging < TargetDB
  self.table_name= "taggings"
end

################################################################################################################################################

class AmazonBackup
  def self.execute

    count=0

    puts "****** Connect Amazon"
# create a connection
    gpg_email=Array.new(1, AWS_S3::GPG_EMAIL_ADDRESS)
    connection= AWS::S3::Base.establish_connection!(:access_key_id => AWS_S3::AWS_S3_ACCESS_KEY, :secret_access_key => AWS_S3::AWS_S3_SECRET_KEY)
    puts "****** Connected: #{connection}"

    Pages.where('backup=0').each do |page|
      count=count+1
      ##  break if count>1 ##############################################################

      puts "**** Upload page #{page.id}"

      source_name=S3_SOURCE_PATH+page.id.to_s+".pdf"
      pgp_name=File.join(Dir.tmpdir, page.id.to_s+".gpg")

      Gpgr::Encrypt.file(source_name, :to => pgp_name).encrypt_using(gpg_email)
      res=AWS::S3::S3Object.store(File.basename(pgp_name), open(pgp_name), AWS_S3::AWS_S3_BUCKET)
      puts res

      File.delete(pgp_name); puts "**** Uploading Page Completed"

      page.backup=true
      page.save!

    end

  end
end


################################################################################################################################################

class Migration

  def self.execute

    puts "****** START Migration"

    puts "****** Create Dummy Cover"

    migration_cover=Cover.create(:folder_id => 9999, :counter => 0)


    puts "Migrating #{Docs.get_titles.count} Documents"

    count=0

    Docs.get_titles.each do |title|

      count=count+1
      break if count>100 ########## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1

      puts "********* Read Title: #{title.id}"

      TargetDB.transaction do

        new_document=Document.new
        new_document.comment=title.comment
        new_document.created_at=title.created_at
        new_document.first_page_only= title.firstpage
        new_document.page_count=title.get_page_count
        new_document.status=0 # page already uploaded to amazon
        new_document.save!

        puts "  Created Document - ID: #{new_document.id}"

        title.get_taggings.each do |stag|
          case stag.tag_id
            when 21 then # Archive 1 year
              new_document.update_attribute(:no_delete, true) ##ARCHIVE FLAG
              puts "    Document flaged as DONT DELETE"
            when 22 then # Archive 2 years
              new_document.delete_at=new_document.created_at+1.year
              new_document.save!
              puts "    Achrive document 1 year until #{new_document.delete_at}"
            when 23 then
              new_document.delete_at=new_document.created_at+2.years
              new_document.save!
              puts "    Achrive document 2 years until #{new_document.delete_at}"
            else
              ttag=TargetTagging.new
              ttag.taggable_type='Document'
              ttag.tag_id=TAG_MAPPING[stag.tag_id] ## remap tags for cleanup
              ttag.taggable_id=new_document.id
              ttag.context=stag.context
              ttag.save!
              puts "    Created Tag - ID: #{ttag.id}"
          end
        end

        id=Pages.new_page(new_document.id, title, migration_cover.id)
        puts "  Created First Page  #{id}"

        title.get_pages.each do |page|
          id = Pages.new_page(new_document.id, page, migration_cover.id)
          puts "  Created Page  #{id}"
        end

      end
    end

    puts "******* DONE"
  end
end

###############################################################

#igration.execute
AmazonBackup.execute