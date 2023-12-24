package Plugins::SlimLibrary::Plugin;

#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#

use strict;

use base qw(Slim::Plugin::Base);
use Scalar::Util qw(blessed);
use Digest::MD5 qw(md5_hex);
use Slim::Music::VirtualLibraries;
use Slim::Control::Request;
use Slim::Utils::Log;
use Slim::Utils::Prefs;
use Slim::Utils::Strings qw(string cstring);
use Slim::Utils::Misc qw( specified );
use Slim::Web::Pages;
use Slim::Schema;
use HTTP::Status qw(
    RC_FORBIDDEN
	RC_PRECONDITION_FAILED
	RC_UNAUTHORIZED
	RC_MOVED_PERMANENTLY
	RC_NOT_FOUND
	RC_METHOD_NOT_ALLOWED
	RC_OK
	RC_NOT_MODIFIED
);
use Slim::Player::ProtocolHandlers;

use Data::Dumper;
use File::Basename;
use List::Util qw(first);
use File::Spec::Functions qw/ canonpath /;
use File::Find::Rule;
use URI::file;


# So, get the data related to this plugin.
# The call to preferences() will be "plugin.<plugin name in lowercase>
# Something that might be interesting, there's actually a prefs file for each plugin.
# On Windows its stored in documents and settings/all users/application data/squeezecenter/prefs.
# You can look at it (it's a text file) and see what is being currently stored.
# However, there seems to be a lag between doing a set and it actually showing up in the file, fyi.
my $prefs = preferences('plugin.slimlibrary');

# Any global variables? Go ahead and declare and/or set them here
use constant SLIMLIBRARY_FILE_URL => 'plugins/SlimLibrary/[0-9]+/';
use constant SLIMLIBRARY_VIDEOTHUMBNAIL_URL => 'plugins/SlimLibrary/videothumbnail/';
use constant IMG_EXTS => ('.jpg','.JPG','.jpeg','.png','.PNG');
use constant SLIMLIBRARY_VERSION => 2.00;

# used for logging
# Log levels are DEBUG, INFO, WARN, ERROR, FATAL where a level will include messages for all levels to the right.
my $log = Slim::Utils::Log->addLogCategory({
	'category' => 'plugin.slimlibraryplugin',
	'defaultLevel' => 'INFO',
	'description' => 'PLUGIN_SLIM_LIBRARY',
});

# Frequently used data can be cached in memory, such as the list of albums for Jive
my $cache = {};

sub myDebug {
	my $msg = shift;
	my $lvl = shift;
	
	if ($lvl eq "") { $lvl = "debug"; }

	$log->$lvl("*** SlimLibrary *** $msg");
}

#Caches folders
#slimlibrary folder
my $cachedir;
#video thumbnail folder
my $thumbNailCacheFolder;


# It returns the name to display on the squeezebox
sub getDisplayName {
	myDebug("getDisplayName() got called, returning 'PLUGIN_SLIM_LIBRARY'");
	return 'PLUGIN_SLIM_LIBRARY';
}

# Everything is handled by the input modes stuff
# So, just return and empty hash - NOTE THE CURLY BRACES with the return call! 
# I spent a while debugging a "bogus function" error because I had return() and not return{}.
# Normally this would return a reference to a functions hash that hashes button presses with actions.
sub getFunctions {
	myDebug("getFunctions() got called, returning nothing");
	return{};
}

# This is called when SC loads the plugin.
# So use it to initialize variables and the like.
sub initPlugin {
	
	myDebug("Initializing the plugin");
		
	$log->debug("SlimLibrary init start\n");
	Slim::Web::Pages->addPageFunction(SLIMLIBRARY_FILE_URL, \&handleHTTP);
    Slim::Web::Pages->addPageFunction(SLIMLIBRARY_VIDEOTHUMBNAIL_URL, \&handleVideoThumbnail);
    
	#CLI commands
	Slim::Control::Request::addDispatch(['slimlibrary','videos'], [0, 1, 0, \&canServeVideo]);
	Slim::Control::Request::addDispatch(['slimlibrary','imagelist','_albumId'], [0, 1, 0, \&imagelist]);
	Slim::Control::Request::addDispatch(['slimlibrary','album_index_list','_albumId'], [0, 1, 0, \&albumIndexList]);
	Slim::Control::Request::addDispatch(['slimlibrary','albums','_index','_quantity'], [0, 1, 1, \&slimAlbumsQuery]);
	Slim::Control::Request::addDispatch(['slimlibrary','video_folders','_index','_quantity'], [0, 1, 1, \&videoFoldersQuery]);
	Slim::Control::Request::addDispatch(['slimlibrary','video_titles','_index','_quantity'], [0, 1, 1, \&videoTitlesQuery]);
	Slim::Control::Request::addDispatch(['slimlibrary','version'], [0, 1, 0, \&versionQuery]);
	Slim::Control::Request::addDispatch(['slimlibrary','mainArtists','_index','_quantity'], [0, 1, 1, \&mainArtistsQuery]);
	Slim::Control::Request::addDispatch(['slimlibrary','albumArtists','_index','_quantity'], [0, 1, 1, \&albumArtistsQuery]);
	Slim::Control::Request::addDispatch(['slimlibrary','compilationArtists','_index','_quantity'], [0, 1, 1, \&compilationArtistsQuery]);
	Slim::Control::Request::addDispatch(['slimlibrary','iconForFavoriteItem','_url'], [0, 1, 1, \&iconForUrl]);
	
	

	#Video Download
	Slim::Web::Pages->addRawDownload('^plugins/SlimLibrary/video/', \&downloadVideo, 'video/mp4');
	
	#Create Video Thumbnail Cached directory if not exists
	$cachedir = File::Spec->catdir(preferences('server')->get('cachedir'), 'slimlibrary');
	$cachedir = decodeUrl($cachedir);
	mkdir $cachedir,0777;
	
	$thumbNailCacheFolder = File::Spec->catdir($cachedir, 'videothumbnails');
	mkdir $thumbNailCacheFolder,0777;
	
	myDebug("Done with Initializing the plugin, waiting for user to do something");

}

sub handleHTTP {
	my ($client, $params, $callback, $httpClient, $httpResponse, $request) = @_;
	
	my $path = $params->{path};
	
	$log->debug("Handle HTTP path : ".$path);
	$log->debug("fileName : ".getFileName($path));
	
	my $fileName = getFileName($path);
	
	if ($fileName eq "getInfo") {
		$log->debug("File name == getInfo");
		return handleGetInfo($client, $params, $callback, $httpClient, $httpResponse, $request);
	}
	else {
		$log->debug("File name != getInfo");
		return handleFile($client, $params, $callback, $httpClient, $httpResponse, $request);
	}
	
}

sub handleVideoThumbnail {
	
	$log->debug("Handling Video Thumbnail");
	
	my ($client, $params, $callback, $httpClient, $httpResponse, $request) = @_;
	
	my $path = $params->{path};
	
	my $videoId = getVideoId($path);
		
	my $file = File::Spec->catfile($thumbNailCacheFolder, $videoId.".png");
	
	$file = decodeUrl($file);
	 
	$log->debug("Video Thumb Nail Url : ".$file);
	
	#Check if file exists
	unless (-e $file) { 
		createThumbnail($videoId,$file);
	}
	
	$log->debug("File : ".$file);
	
	my ($name,$path,$suffix) = fileparse($file, qr/\.[^.]*/);

	my $content = do {
		local $/ = undef;
		open my $fh, "<", $file or $log->error("could not open $file: $!");
		if( $fh) {
			binmode $fh;
			<$fh>;
		}
	};

	$httpResponse->header( 'Content-Length' => length($content) );
	if( $suffix =~ /.png/i ) {
		$httpResponse->header( 'Content-Type' => "image/png" );
	} else {
		$httpResponse->header( 'Content-Type' => "image/jpg" );
	}
	return \$content;
}

sub createThumbnail {
	
	#find ffmpeg on system
	my $ffmpeg = Slim::Utils::Misc::findbin('ffmpeg');
	

	my $videoId = shift;
	my $file = shift;
	
		#escape back slashes
	$file =~ s/\\/\\\\/g;
		
	$log->debug("Creating thumbnail at : ".$file);
	
	my $video = getVideoById($videoId);
	
	my $videoUrl = decodeFileUrl($video->{'url'});
			
	#escape back slashes
	$videoUrl =~ s/\\/\\\\/g;
				
    $log->debug("Video Url : ".$videoUrl);
	    
    my @args = ("-i", $videoUrl, "-ss", "00:00:20", "-vframes", "1", $file);
		
	system ("/Library/ffmpeg/ffmpeg",@args);
	
}

sub downloadVideo {
	my $path = shift;
	
	$log->debug("Path : ".$path);

	$log->debug("Video Id : ".getVideoId($path));
	
	my $video = getVideoById(getVideoId($path));
	
	$log->debug("Video Info :".Dumper($video));
	
    my $videoUrl = decodeFileUrl($video->{'url'});
    $log->debug("Video Url : ".$videoUrl);
    
	return $videoUrl;

}
#####################################################
# Get plugin version
#####################################################
sub versionQuery {
	my $request = shift;

#	# check this is the correct query.
#	if ($request->isNotQuery([['version']])) {
#		$request->setStatusBadDispatch();
#		return;
#	}

	# no params for the version query

	$request->addResult('_version', SLIMLIBRARY_VERSION);
	
	$request->setStatusDone();
}

#####################################################
# Get distinct video folders
#####################################################
sub videoFoldersQuery {
	my $request = shift;

	if (!Slim::Schema::hasLibrary()) {
		$request->setStatusNotDispatchable();
		return;
	}
	
	my $sqllog = main::DEBUGLOG && logger('database.sql');
	
	# get our parameters
	my $index         = $request->getParam('_index');
	my $quantity      = $request->getParam('_quantity');
	
		
	my $collate = Slim::Utils::OSDetect->getOS()->sqlHelperClass()->collate();
	
	my $sql      = 'SELECT DISTINCT album FROM videos ';
	my $c        = { 'videos.album' =>1 };
	my $p        = [];
	
	$sql .= "ORDER BY album ";
	
	my @cols = keys %{$c};
	$sql = sprintf $sql, join( ', ', map { $_ . " AS '" . $_ . "'" } @cols );
	
	my $stillScanning = Slim::Music::Import->stillScanning();
	
	my $dbh = Slim::Schema->dbh;
	
	# Get count of all results, the count is cached until the next rescan done event
	my $cacheKey = $sql . join( '', @{$p} );
	
	my ($count) = $cache->{$cacheKey} || $dbh->selectrow_array( qq{
		SELECT COUNT(*) FROM ( $sql ) AS t1
	}, undef, @{$p} );
	
	if ( !$stillScanning ) {
		$cache->{$cacheKey} = $count;
	}

	if ($stillScanning) {
		$request->addResult('rescan', 1);
	}

	$count += 0;

	my $totalCount = $count;
	# now build the result
	my ($valid, $start, $end) = $request->normalize(scalar($index), scalar($quantity), $count);

	my $loopname = 'videos_loop';
	my $chunkCount = 0;
	
#	my $valid = 1;

	if ($valid) {		
		# Limit the real query
		if ( $index =~ /^\d+$/ && $quantity =~ /^\d+$/ ) {
			$sql .= "LIMIT $index, $quantity ";
		}
		
		$log->debug("Video Folders query: $sql / " . Data::Dump::dump($p) );

		my $sth = $dbh->prepare_cached($sql);
		$sth->execute( @{$p} );
		
		# Bind selected columns in order
		my $i = 1;
		for my $col ( @cols ) {
			$sth->bind_col( $i++, \$c->{$col} );
		}
		
		while ( $sth->fetch ) {

			# "raw" result formatting (for CLI or JSON RPC)			
			$request->addResultLoop($loopname, $chunkCount, 'album', $c->{'videos.album'});
			
			$chunkCount++;
			
			main::idleStreams() if !($chunkCount % 5);
		}
	}

	$request->addResult('count', $totalCount);
	
	$request->setStatusDone();
}

###########################################################
# Video Query
# Enhanced from original to allow searching by folder name
###########################################################

sub videoTitlesQuery {
	my $request = shift;

$log->debug("VideoTitles query" );

	if (!Slim::Schema::hasLibrary()) {
		$request->setStatusNotDispatchable();
		return;
	}
	
	my $sqllog = main::DEBUGLOG && logger('database.sql');
	
	# get our parameters
	my $index         = $request->getParam('_index');
	my $quantity      = $request->getParam('_quantity');
	my $tags          = $request->getParam('tags') || 't';
	my $search        = $request->getParam('search');
	my $sort          = $request->getParam('sort');
	my $videoHash     = $request->getParam('video_id');
	my $album		  = $request->getParam('album');
	
	#if ($sort && $request->paramNotOneOfIfDefined($sort, ['new'])) {
	#	$request->setStatusBadParams();
	#	return;
	#}

	my $collate = Slim::Utils::OSDetect->getOS()->sqlHelperClass()->collate();
	
	my $sql      = 'SELECT %s FROM videos ';
	my $c        = { 'videos.hash' => 1, 'videos.titlesearch' => 1, 'videos.titlesort' => 1 };
	my $w        = [];
	my $p        = [];
	my $order_by = "videos.titlesort $collate";
	my $limit;
	
	# Normalize and add any search parameters
	if ( defined $videoHash ) {
		push @{$w}, 'videos.hash = ?';
		push @{$p}, $videoHash;
	}
	# ignore everything if $videoID was specified
	else {
		if ($sort) {
			if ( $sort eq 'new' ) {
				$limit = $prefs->get('browseagelimit') || 100;
				$order_by = "videos.added_time desc";

				# Force quantity to not exceed max
				if ( $quantity && $quantity > $limit ) {
					$quantity = $limit;
				}
			}
			elsif ( $sort =~ /^sql=(.+)/ ) {
				$order_by = $1;
				$order_by =~ s/;//g; # strip out any attempt at combining SQL statements
			}
		}
	
	
	################################################################
	# This was the only change required LVW
	################################################################
		$search = decodeUrl($search);		
	################################################################

		if ( $search && specified($search) ) {
			if ( $search =~ s/^sql=// ) {
				# Raw SQL search query
				$search =~ s/;//g; # strip out any attempt at combining SQL statements
				push @{$w}, $search;
			}
			else {
				my $strings = Slim::Utils::Text::searchStringSplit($search);
				if ( ref $strings->[0] eq 'ARRAY' ) {
					push @{$w}, '(' . join( ' OR ', map { 'videos.titlesearch LIKE ?' } @{ $strings->[0] } ) . ')';
					push @{$p}, @{ $strings->[0] };
				}
				else {		
					push @{$w}, 'videos.titlesearch LIKE ?';
					push @{$p}, @{$strings};
				}
			}
		}
	}
	
	$tags =~ /t/ && do { $c->{'videos.title'} = 1 };
	$tags =~ /d/ && do { $c->{'videos.secs'} = 1 };
	$tags =~ /o/ && do { $c->{'videos.mime_type'} = 1 };
	$tags =~ /r/ && do { $c->{'videos.bitrate'} = 1 };
	$tags =~ /f/ && do { $c->{'videos.filesize'} = 1 };
	$tags =~ /w/ && do { $c->{'videos.width'} = 1 };
	$tags =~ /h/ && do { $c->{'videos.height'} = 1 };
	$tags =~ /n/ && do { $c->{'videos.mtime'} = 1 };
	$tags =~ /F/ && do { $c->{'videos.dlna_profile'} = 1 };
	$tags =~ /D/ && do { $c->{'videos.added_time'} = 1 };
	$tags =~ /U/ && do { $c->{'videos.updated_time'} = 1 };
	$tags =~ /l/ && do { $c->{'videos.album'} = 1 };

	if ( @{$w} ) {
		$sql .= 'WHERE ';
		$sql .= join( ' AND ', @{$w} );
		$sql .= ' ';
	}
	$sql .= "GROUP BY videos.hash ORDER BY $order_by ";
	
	# Add selected columns
	# Bug 15997, AS mapping needed for MySQL
	my @cols = keys %{$c};
	$sql = sprintf $sql, join( ', ', map { $_ . " AS '" . $_ . "'" } @cols );
	
	my $stillScanning = Slim::Music::Import->stillScanning();
	
	my $dbh = Slim::Schema->dbh;
	
	# Get count of all results, the count is cached until the next rescan done event
	my $cacheKey = $sql . join( '', @{$p} );
	
	my ($count) = $cache->{$cacheKey} || $dbh->selectrow_array( qq{
		SELECT COUNT(*) FROM ( $sql ) AS t1
	}, undef, @{$p} );
	
	if ( !$stillScanning ) {
		$cache->{$cacheKey} = $count;
	}

	if ($stillScanning) {
		$request->addResult('rescan', 1);
	}

	$count += 0;

	my $totalCount = $count;

	# now build the result
	my ($valid, $start, $end) = $request->normalize(scalar($index), scalar($quantity), $count);

	my $loopname = 'videos_loop';
	my $chunkCount = 0;

	if ($valid) {		
		# Limit the real query
		if ( $index =~ /^\d+$/ && $quantity =~ /^\d+$/ ) {
			$sql .= "LIMIT $index, $quantity ";
		}

		if ( main::DEBUGLOG && $sqllog->is_debug ) {
			$sqllog->debug( "Video Titles query: $sql / " . Data::Dump::dump($p) );
		}
		
		my $sth = $dbh->prepare_cached($sql);
		$sth->execute( @{$p} );
		
		# Bind selected columns in order
		my $i = 1;
		for my $col ( @cols ) {
			$sth->bind_col( $i++, \$c->{$col} );
		}
		
		while ( $sth->fetch ) {
			if ( $sort ne 'new' ) {
				utf8::decode( $c->{'videos.titlesort'} ) if exists $c->{'videos.titlesort'};
			}

			# "raw" result formatting (for CLI or JSON RPC)
			$request->addResultLoop($loopname, $chunkCount, 'id', $c->{'videos.hash'});				

			_videoData($request, $loopname, $chunkCount, $tags, $c);
		
			$chunkCount++;
			
			main::idleStreams() if !($chunkCount % 5);
		}
	}

	$request->addResult('count', $totalCount);
	
	$request->setStatusDone();
}

sub _videoData {
	my ($request, $loopname, $chunkCount, $tags, $c) = @_;

	utf8::decode( $c->{'videos.title'} ) if exists $c->{'videos.title'};
	utf8::decode( $c->{'videos.album'} ) if exists $c->{'videos.album'};

	$tags =~ /t/ && $request->addResultLoop($loopname, $chunkCount, 'title', $c->{'videos.title'});
	$tags =~ /d/ && $request->addResultLoop($loopname, $chunkCount, 'duration', $c->{'videos.secs'});
	$tags =~ /o/ && $request->addResultLoop($loopname, $chunkCount, 'mime_type', $c->{'videos.mime_type'});
	$tags =~ /r/ && $request->addResultLoop($loopname, $chunkCount, 'bitrate', $c->{'videos.bitrate'} / 1000);
	$tags =~ /f/ && $request->addResultLoop($loopname, $chunkCount, 'filesize', $c->{'videos.filesize'});
	$tags =~ /w/ && $request->addResultLoop($loopname, $chunkCount, 'width', $c->{'videos.width'});
	$tags =~ /h/ && $request->addResultLoop($loopname, $chunkCount, 'height', $c->{'videos.height'});
	$tags =~ /n/ && $request->addResultLoop($loopname, $chunkCount, 'mtime', $c->{'videos.mtime'});
	$tags =~ /F/ && $request->addResultLoop($loopname, $chunkCount, 'dlna_profile', $c->{'videos.dlna_profile'});
	$tags =~ /D/ && $request->addResultLoop($loopname, $chunkCount, 'added_time', $c->{'videos.added_time'});
	$tags =~ /U/ && $request->addResultLoop($loopname, $chunkCount, 'updated_time', $c->{'videos.updated_time'});
	$tags =~ /l/ && $request->addResultLoop($loopname, $chunkCount, 'album', $c->{'videos.album'});
	$tags =~ /J/ && $request->addResultLoop($loopname, $chunkCount, 'hash', $c->{'videos.hash'});
}

#####################################################
# Slim new albums query
#####################################################
sub slimAlbumsQuery {
	my $request = shift;

	# check this is the correct query.
#	if ($request->isNotQuery([['albums']])) {
#		$request->setStatusBadDispatch();
#		return;
#	}

	if (!Slim::Schema::hasLibrary()) {
		$request->setStatusNotDispatchable();
		return;
	}
	
	my $sqllog = main::DEBUGLOG && logger('database.sql');
	
	# get our parameters
	my $client        = $request->client();
	my $index         = $request->getParam('_index');
	my $quantity      = $request->getParam('_quantity');
	my $tags          = $request->getParam('tags') || 'l';
	my $search        = $request->getParam('search');
	my $compilation   = $request->getParam('compilation');
	my $contributorID = $request->getParam('artist_id');
	my $genreID       = $request->getParam('genre_id');
	my $trackID       = $request->getParam('track_id');
	my $albumID       = $request->getParam('album_id');
	my $roleID        = $request->getParam('role_id');
	my $libraryID     = Slim::Music::VirtualLibraries->getRealId($request->getParam('library_id'));
	my $year          = $request->getParam('year');
	my $sort          = $request->getParam('sort') || ($roleID ? 'artistalbum' : 'album');
	
	############################################################
	# new sort order parameter
	my $sortOrder	  = $request->getParam('sort_order') || 'ASC';
	############################################################
	

	my $ignoreNewAlbumsCache = $search || $compilation || $contributorID || $genreID || $trackID || $albumID || $year || Slim::Music::Import->stillScanning();
	
	# FIXME: missing genrealbum, genreartistalbum
	# Add dateadded param
	#######################################################################################################################################
	#if ($request->paramNotOneOfIfDefined($sort, ['new', 'album', 'artflow', 'artistalbum', 'yearalbum', 'yearartistalbum', 'random' ])) {
	if ($request->paramNotOneOfIfDefined($sort, ['new', 'album', 'artflow', 'artistalbum', 'yearalbum', 'yearartistalbum', 'random','dateadded' ])) {
	########################################################################################################################################	
		$request->setStatusBadParams();
		return;
	}

	my $collate = Slim::Utils::OSDetect->getOS()->sqlHelperClass()->collate();
	
	my $sql      = 'SELECT %s FROM albums ';
	my $c        = { 'albums.id' => 1, 'albums.titlesearch' => 1, 'albums.titlesort' => 1 };
	my $w        = [];
	my $p        = [];
	my $order_by = "albums.titlesort $collate, albums.disc"; # XXX old code prepended 0 to titlesort, but not other titlesorts
	my $limit;
	my $page_key = "SUBSTR(albums.titlesort,1,1)";
	my $newAlbumsCacheKey = 'newAlbumIds' . Slim::Music::Import->lastScanTime . Slim::Music::VirtualLibraries->getLibraryIdForClient($client);
	
	######################################
	# set sort order descending
	######################################
	if ( $sortOrder eq 'DESC' ) {
			$order_by = "albums.titlesort $collate desc, albums.disc";
	}
	#######################################
	
	# Normalize and add any search parameters
	if ( defined $trackID ) {
		$sql .= 'JOIN tracks ON tracks.album = albums.id ';
		push @{$w}, 'tracks.id = ?';
		push @{$p}, $trackID;
	}
	elsif ( defined $albumID ) {
		push @{$w}, 'albums.id = ?';
		push @{$p}, $albumID;
	}
	# ignore everything if $track_id or $album_id was specified
	else {
		if (specified($search)) {
			if ( Slim::Schema->canFulltextSearch ) {
				Slim::Plugin::FullTextSearch::Plugin->createHelperTable({
					name   => 'albumsSearch',
					search => $search,
					type   => 'album',
				});
				
				$sql = 'SELECT %s FROM albumsSearch, albums ';
				unshift @{$w}, "albums.id = albumsSearch.id";
				
				if ($tags ne 'CC') {
					$order_by = $sort = "albumsSearch.fulltextweight DESC, LENGTH(albums.titlesearch)";
				}
			}
			else {
				my $strings = Slim::Utils::Text::searchStringSplit($search);
				if ( ref $strings->[0] eq 'ARRAY' ) {
					push @{$w}, '(' . join( ' OR ', map { 'albums.titlesearch LIKE ?' } @{ $strings->[0] } ) . ')';
					push @{$p}, @{ $strings->[0] };
				}
				else {		
					push @{$w}, 'albums.titlesearch LIKE ?';
					push @{$p}, @{$strings};
				}
			}
		}

		my @roles;
		if (defined $contributorID) {
			# handle the case where we're asked for the VA id => return compilations
			if ($contributorID == Slim::Schema->variousArtistsObject->id) {
				$compilation = 1;
			}
			else {

				$sql .= 'JOIN contributor_album ON contributor_album.album = albums.id ';
				#push @{$w}, 'contributor_album.contributor = ?';
				#push @{$p}, $contributorID;

				##########################################################
				# Enhancement to allow for multiple artist id
				###########################################################
				my $whereClause = 'contributor_album.contributor IN (';
				my @contributors = split(/,/,$contributorID);
				
				foreach my $contrib (@contributors) {
					
					$whereClause .= '?,';
					push @{$p}, $contrib;
				}
				chop $whereClause;
				
				$whereClause .= ')';
				push @{$w}, $whereClause;
				
				$log->debug("Artists where clause  / " .$whereClause);
				############################################################
				
				# only albums on which the contributor has a specific role?
				if ($roleID) {
					@roles = split /,/, $roleID;
					push @roles, 'ARTIST' if $roleID eq 'ALBUMARTIST' && !$prefs->get('useUnifiedArtistsList');
				}
				elsif ($prefs->get('useUnifiedArtistsList')) {
					@roles = ( 'ARTIST', 'TRACKARTIST', 'ALBUMARTIST' );
			
					# Loop through each pref to see if the user wants to show that contributor role.
					foreach (Slim::Schema::Contributor->contributorRoles) {
						if ($prefs->get(lc($_) . 'InArtists')) {
							push @roles, $_;
						}
					}
				}
				else {
					@roles = Slim::Schema::Contributor->contributorRoles();
				}
			}	
		}
		elsif ($roleID) {
			$sql .= 'JOIN contributor_album ON contributor_album.album = albums.id ';

			@roles = split /,/, $roleID;
			push @roles, 'ARTIST' if $roleID eq 'ALBUMARTIST' && !$prefs->get('useUnifiedArtistsList');
		}
		
		if (scalar @roles) {
			push @{$p}, map { Slim::Schema::Contributor->typeToRole($_) } @roles;
			push @{$w}, 'contributor_album.role IN (' . join(', ', map {'?'} @roles) . ')';
			
			$sql .= 'JOIN contributors ON contributors.id = contributor_album.contributor ';
		}
		elsif ( $sort =~ /artflow|artistalbum/) {
			$sql .= 'JOIN contributors ON contributors.id = albums.contributor ';
		}
		
		$log->debug("SQL before id new =  ".$sql);
	
		if ( $sort eq 'new' ) {
			$sql .= 'JOIN tracks ON tracks.album = albums.id ';
			$limit = $prefs->get('browseagelimit') || 100;
			$order_by = "tracks.timestamp desc";
			
			# Force quantity to not exceed max
			if ( $quantity && $quantity > $limit ) {
				$quantity = $limit;
			}

			# cache the most recent album IDs - need to query the tracks table, which is expensive
			if ( !$ignoreNewAlbumsCache ) {
				my $ids = $cache->{$newAlbumsCacheKey} || [];
				
				if (!scalar @$ids) {
					my $_cache = Slim::Utils::Cache->new;
					$ids = $_cache->get($newAlbumsCacheKey) || [];

					# get rid of stale cache entries
					my @oldCacheKeys = grep /newAlbumIds/, keys %$cache;
					foreach (@oldCacheKeys) {
						next if $_ eq $newAlbumsCacheKey;
						$_cache->remove($_);
						delete $cache->{$_};
					}

					my $countSQL = qq{
						SELECT tracks.album
						FROM tracks } . ($libraryID ? qq{
							JOIN library_track ON library_track.library = '$libraryID' AND tracks.id = library_track.track
						} : '') . qq{
						WHERE tracks.album > 0
						GROUP BY tracks.album
						ORDER BY tracks.timestamp DESC
					};

					# get the list of album IDs ordered by timestamp
					$ids = Slim::Schema->dbh->selectcol_arrayref( $countSQL, { Slice => {} } ) unless scalar @$ids;
					
					$cache->{$newAlbumsCacheKey} = $ids;
					$_cache->set($newAlbumsCacheKey, $ids, 86400 * 7) if scalar @$ids;
				}

				my $start = scalar($index);
				my $end   = $start + scalar($quantity || scalar($limit)-1);
				if ($end >= scalar @$ids) {
					$end = scalar(@$ids) - 1;
				}
				push @{$w}, 'albums.id IN (' . join(',', @$ids[$start..$end]) . ')';

				# reset $index, as we're already limiting results using the id list
				$index = 0;
			}

			$page_key = undef;
		}
		elsif ( $sort eq 'artflow' ) {
			#$order_by = "contributors.namesort $collate, albums.year, albums.titlesort $collate";
			#######################################################################################
			if ( $sortOrder eq 'DESC' ) {
				$order_by = "contributors.namesort $collate desc, albums.year, albums.titlesort $collate";
			}
			else {
				$order_by = "contributors.namesort $collate, albums.year, albums.titlesort $collate";
			}
			########################################################################################
			$c->{'contributors.namesort'} = 1;
			$page_key = "SUBSTR(contributors.namesort,1,1)";
		}
		elsif ( $sort eq 'artistalbum' ) {
			#$order_by = "contributors.namesort $collate, albums.titlesort $collate";
			#######################################################################################
			if ( $sortOrder eq 'DESC' ) {
				$order_by = "contributors.namesort $collate desc, albums.titlesort $collate";
			}
			else {
				$order_by = "contributors.namesort $collate, albums.titlesort $collate";
			}
			######################################################################################
			$c->{'contributors.namesort'} = 1;
			$page_key = "SUBSTR(contributors.namesort,1,1)";
		}
		elsif ( $sort eq 'yearartistalbum' ) {
			#$order_by = "albums.year, contributors.namesort $collate, albums.titlesort $collate";
			#######################################################################################
			if ( $sortOrder eq 'DESC' ) {
				$order_by = "albums.year desc, contributors.namesort $collate, albums.titlesort $collate";
			}
			else {
				$order_by = "albums.year, contributors.namesort $collate, albums.titlesort $collate";
			}
			###########################################################################################
			$page_key = "albums.year";
		}
		elsif ( $sort eq 'yearalbum' ) {
			#$order_by = "albums.year, albums.titlesort $collate";
			###########################################################################################
			if ( $sortOrder eq 'DESC' ) {
				$order_by = "albums.year desc, albums.titlesort $collate";
			}
			else {
				$order_by = "albums.year, albums.titlesort $collate";
			}
			#############################################################################################
			$page_key = "albums.year";
		}
		elsif ( $sort eq 'random' ) {
			$limit = $prefs->get('itemsPerPage');
			
			# Force quantity to not exceed max
			if ( $quantity && $quantity > $limit ) {
				$quantity = $limit;
			}

			$order_by = Slim::Utils::OSDetect->getOS()->sqlHelperClass()->randomFunction();
			$page_key = undef;
		}
		#########################################################################
		# New Date Added sort order
		########################################################################		
		elsif ( $sort eq 'dateadded' ) {
			$sql .= 'JOIN tracks ON tracks.album = albums.id ';
			if ( $sortOrder eq 'DESC' ) {
				$order_by = "tracks.timestamp desc, tracks.disc, tracks.tracknum, tracks.titlesort $collate";
			}
			else {
				$order_by = "tracks.timestamp, tracks.disc, tracks.tracknum, tracks.titlesort $collate";
			}
			$c->{'tracks.timestamp'} = 1;
			#$page_key = "tracks.timestamp";
		}		
		############################################################################

		if (defined $libraryID) {
			push @{$w}, 'albums.id IN (SELECT library_album.album FROM library_album WHERE library_album.library = ?)';
			push @{$p}, $libraryID;
		}
		
		if (defined $year) {
			#push @{$w}, 'albums.year = ?';
			#push @{$p}, $year;
			
				##########################################################
				# Enhancement to allow for multiple years
				###########################################################
				my $whereClause = 'albums.year IN (';
				my @parts = split(/,/,$year);
				
				foreach my $part (@parts) {
					
					$whereClause .= '?,';
					push @{$p}, $part;
				}
				
				#get rid of extra comma at end
				chop $whereClause;
				
				$whereClause .= ')';
				push @{$w}, $whereClause;
				
				$log->debug("Years where clause  / " .$whereClause);
				############################################################
		}
		
		if (defined $genreID) {
			my @genreIDs = split(/,/, $genreID);
			$sql .= 'JOIN tracks ON tracks.album = albums.id ' unless $sql =~ /JOIN tracks/;
			$sql .= 'JOIN genre_track ON genre_track.track = tracks.id ';
			#push @{$w}, 'genre_track.genre IN (' . join(', ', map {'?'} @genreIDs) . ')';
			#push @{$p}, @genreIDs;
			
				##########################################################
				# Enhancement to allow for multiple genres
				###########################################################
				my $whereClause = 'genre_track.genre IN (';
				my @parts = split(/,/,$genreID);
				
				foreach my $part (@parts) {
					$whereClause .= '?,';
					push @{$p}, $part;
				}
				chop $whereClause;
				
				$whereClause .= ')';
				push @{$w}, $whereClause;
				
				$log->debug("Genre where clause  / " .$whereClause);
				############################################################
		}
	
		if (defined $compilation) {
			if ($compilation == 1) {
				push @{$w}, 'albums.compilation = 1';
			}
			else {
				push @{$w}, '(albums.compilation IS NULL OR albums.compilation = 0)';
			}
		}
	}
	
	if ( $tags =~ /l/ ) {
		# title/disc/discc is needed to construct (N of M) title
		map { $c->{$_} = 1 } qw(albums.title albums.disc albums.discc);
	}
	
	if ( $tags =~ /y/ ) {
		$c->{'albums.year'} = 1;
	}
	
	if ( $tags =~ /j/ ) {
		$c->{'albums.artwork'} = 1;
	}
	
	if ( $tags =~ /t/ ) {
		$c->{'albums.title'} = 1;
	}
	
	if ( $tags =~ /i/ ) {
		$c->{'albums.disc'} = 1;
	}
	
	if ( $tags =~ /q/ ) {
		$c->{'albums.discc'} = 1;
	}
	
	if ( $tags =~ /w/ ) {
		$c->{'albums.compilation'} = 1;
	}

	if ( $tags =~ /X/ ) {
		$c->{'albums.replay_gain'} = 1;
	}

	if ( $tags =~ /S/ ) {
		$c->{'albums.contributor'} = 1;
	}

	if ( $tags =~ /a/ ) {
		# If requesting artist data, join contributor
		if ( $sql !~ /JOIN contributors/ ) {
			if ( $sql =~ /JOIN contributor_album/ ) {
				# Bug 17364, if looking for an artist_id value, we need to join contributors via contributor_album
				# or No Album will not be found properly
				$sql .= 'JOIN contributors ON contributors.id = contributor_album.contributor ';
			}
			else {
				$sql .= 'JOIN contributors ON contributors.id = albums.contributor ';
			}
		}
		$c->{'contributors.name'} = 1;
		
		# if albums for a specific contributor are requested, then we need the album's contributor, too
		$c->{'albums.contributor'} = $contributorID;
	}
	
	if ( $tags =~ /s/ ) {
		$c->{'albums.titlesort'} = 1;
	}
	
	if ( @{$w} ) {
		$sql .= 'WHERE ';
		my $s .= join( ' AND ', @{$w} );
		$s =~ s/\%/\%\%/g;
		$sql .= $s . ' ';
	}
	
	my $dbh = Slim::Schema->dbh;

	$sql .= "GROUP BY albums.id ";
	
	if ($page_key && $tags =~ /Z/) {
		my $pageSql = "SELECT n, count(1) FROM ("
			. sprintf($sql, "$page_key AS n")
			. ") AS pk GROUP BY n ORDER BY n " . ($sort !~ /year/ ? "$collate " : '');

		if ( main::DEBUGLOG && $sqllog->is_debug ) {
			$sqllog->debug( "Albums indexList query: $pageSql / " . Data::Dump::dump($p) );
		}

		$request->addResult('indexList', $dbh->selectall_arrayref($pageSql, undef, @{$p}));
		
		if ($tags =~ /ZZ/) {
			$request->setStatusDone();
			return
		}
	}
	
	$sql .= "ORDER BY $order_by " unless $tags eq 'CC';
	
	# Add selected columns
	# Bug 15997, AS mapping needed for MySQL
	my @cols = sort keys %{$c};
	$sql = sprintf $sql, join( ', ', map { $_ . " AS '" . $_ . "'" } @cols );
	
	my $stillScanning = Slim::Music::Import->stillScanning();
	
	# Get count of all results, the count is cached until the next rescan done event
	my $cacheKey = md5_hex($sql . join( '', @{$p} ) . Slim::Music::VirtualLibraries->getLibraryIdForClient($client) . ($search || ''));
	
	if ( $sort eq 'new' && $cache->{$newAlbumsCacheKey} && !$ignoreNewAlbumsCache ) {
		my $albumCount = scalar @{$cache->{$newAlbumsCacheKey}};
		$albumCount    = $limit if ($limit && $limit < $albumCount);
		$cache->{$cacheKey} ||= $albumCount;
		$limit = undef;
	}
	
	my $countsql = $sql;
	$countsql .= ' LIMIT ' . $limit if $limit;
	
	my $count = $cache->{$cacheKey};
	
	if ( !$count ) {
		my $total_sth = $dbh->prepare_cached( qq{
			SELECT COUNT(1) FROM ( $countsql ) AS t1
		} );

		if ( main::DEBUGLOG && $sqllog->is_debug ) {
			$sqllog->debug( "Albums totals query: $countsql / " . Data::Dump::dump($p) );
		}
		
		$total_sth->execute( @{$p} );
		($count) = $total_sth->fetchrow_array();
		$total_sth->finish;
	}

	if ( !$stillScanning ) {
		$cache->{$cacheKey} = $count;
	}

	if ($stillScanning) {
		$request->addResult('rescan', 1);
	}

	$count += 0;

	# now build the result
	my ($valid, $start, $end) = $request->normalize(scalar($index), scalar($quantity), $count);

	my $loopname = 'albums_loop';
	my $chunkCount = 0;

#######################################
$log->debug("Got this far valid =  ".$valid);
$log->debug("index =  ".$index);
$log->debug("quantity = ".$quantity);
$log->debug("count = ".$count);





######################################################
	if ($valid && $tags ne 'CC') {

		# We need to know the 'No album' name so that those items
		# which have been grouped together under it do not get the
		# album art of the first album.
		# It looks silly to go to Madonna->No album and see the
		# picture of '2 Unlimited'.
		my $noAlbumName = $request->string('NO_ALBUM');
		
		# Limit the real query
		if ($limit && !$quantity) {
			$quantity = "$limit";
			$index ||= "0";
		}
		if ( $index =~ /^\d+$/ && defined $quantity && $quantity =~ /^\d+$/ ) {
			$sql .= "LIMIT ?,? ";
			push @$p, $index, $quantity;
		}

		##############################################################
		$log->debug("Albums query: $sql / " . Data::Dump::dump($p));
		##############################################################
		
		if ( main::DEBUGLOG && $sqllog->is_debug ) {
			$sqllog->debug( "Albums query: $sql / " . Data::Dump::dump($p) );
		}

		my $sth = $dbh->prepare_cached($sql);
		$sth->execute( @{$p} );
		
		# Bind selected columns in order
		my $i = 1;
		for my $col ( @cols ) {
			$sth->bind_col( $i++, \$c->{$col} );
		}

		# Little function to construct nice title from title/disc counts
		my $groupdiscs_pref = $prefs->get('groupdiscs');
		my $construct_title = $groupdiscs_pref ? sub {
			return $c->{'albums.title'};
		} : sub {
			return Slim::Music::Info::addDiscNumberToAlbumTitle(
				$c->{'albums.title'}, $c->{'albums.disc'}, $c->{'albums.discc'}
			);
		};

		my ($contributorSql, $contributorSth, $contributorNameSth);
		if ( $tags =~ /(?:aa|SS)/ ) {
			my @roles = ( 'ARTIST', 'ALBUMARTIST' );
			
			if ($prefs->get('useUnifiedArtistsList')) {
				# Loop through each pref to see if the user wants to show that contributor role.
				foreach (Slim::Schema::Contributor->contributorRoles) {
					if ($prefs->get(lc($_) . 'InArtists')) {
						push @roles, $_;
					}
				}
			}

			$contributorSql = sprintf( qq{
				SELECT GROUP_CONCAT(contributors.name, ',') AS name, GROUP_CONCAT(contributors.id, ',') AS id
				FROM contributor_album
				JOIN contributors ON contributors.id = contributor_album.contributor
				WHERE contributor_album.album = ? AND contributor_album.role IN (%s) 
				GROUP BY contributor_album.role
				ORDER BY contributor_album.role DESC
			}, join(',', map { Slim::Schema::Contributor->typeToRole($_) } @roles) );
		}
		
		my $vaObjId = Slim::Schema->variousArtistsObject->id;
		
		while ( $sth->fetch ) {
			
			utf8::decode( $c->{'albums.title'} ) if exists $c->{'albums.title'};
			
			$request->addResultLoop($loopname, $chunkCount, 'id', $c->{'albums.id'});				
			$tags =~ /l/ && $request->addResultLoop($loopname, $chunkCount, 'album', $construct_title->());
			$tags =~ /y/ && $request->addResultLoopIfValueDefined($loopname, $chunkCount, 'year', $c->{'albums.year'});
			$tags =~ /j/ && $request->addResultLoopIfValueDefined($loopname, $chunkCount, 'artwork_track_id', $c->{'albums.artwork'});
			$tags =~ /t/ && $request->addResultLoop($loopname, $chunkCount, 'title', $c->{'albums.title'});
			$tags =~ /i/ && $request->addResultLoopIfValueDefined($loopname, $chunkCount, 'disc', $c->{'albums.disc'});
			$tags =~ /q/ && $request->addResultLoopIfValueDefined($loopname, $chunkCount, 'disccount', $c->{'albums.discc'});
			$tags =~ /w/ && $request->addResultLoopIfValueDefined($loopname, $chunkCount, 'compilation', $c->{'albums.compilation'});
			$tags =~ /X/ && $request->addResultLoopIfValueDefined($loopname, $chunkCount, 'album_replay_gain', $c->{'albums.replay_gain'});
			$tags =~ /S/ && $request->addResultLoopIfValueDefined($loopname, $chunkCount, 'artist_id', $contributorID || $c->{'albums.contributor'});

			if ($tags =~ /a/) {
				# Bug 15313, this used to use $eachitem->artists which
				# contains a lot of extra logic.

				# Bug 17542: If the album artist is different from the current track's artist,
				# use the album artist instead of the track artist (if available)
				if ($contributorID && $c->{'albums.contributor'} && $contributorID != $c->{'albums.contributor'}) {
					$contributorNameSth ||= $dbh->prepare_cached('SELECT name FROM contributors WHERE id = ?');
					my ($name) = @{ $dbh->selectcol_arrayref($contributorNameSth, undef, $c->{'albums.contributor'}) };
					$c->{'contributors.name'} = $name if $name;
				}

				utf8::decode( $c->{'contributors.name'} ) if exists $c->{'contributors.name'};

				$request->addResultLoopIfValueDefined($loopname, $chunkCount, 'artist', $c->{'contributors.name'});
			}

			if ($tags =~ /s/) {
				#FIXME: see if multiple char textkey is doable for year/genre sort
				my $textKey;
				if ($sort eq 'artflow' || $sort eq 'artistalbum') {
					utf8::decode( $c->{'contributors.namesort'} ) if exists $c->{'contributors.namesort'};
					$textKey = substr $c->{'contributors.namesort'}, 0, 1;
				} elsif ( $sort eq 'album' ) {
					utf8::decode( $c->{'albums.titlesort'} ) if exists $c->{'albums.titlesort'};
					$textKey = substr $c->{'albums.titlesort'}, 0, 1;
				}
				$request->addResultLoopIfValueDefined($loopname, $chunkCount, 'textkey', $textKey);
			}

			# want multiple artists?
			if ( $contributorSql && $c->{'albums.contributor'} != $vaObjId && !$c->{'albums.compilation'} ) {
				$contributorSth ||= $dbh->prepare_cached($contributorSql);
				$contributorSth->execute($c->{'albums.id'});
				
				my $contributor = $contributorSth->fetchrow_hashref;
				$contributorSth->finish;
				
				# XXX - what if the artist name itself contains ','?
				if ( $tags =~ /aa/ && $contributor->{name} ) {
					utf8::decode($contributor->{name});
					$request->addResultLoopIfValueDefined($loopname, $chunkCount, 'artists', $contributor->{name});
				}

				if ( $tags =~ /SS/ && $contributor->{id} ) {
					$request->addResultLoopIfValueDefined($loopname, $chunkCount, 'artist_ids', $contributor->{id});
				}
			}
			
			$chunkCount++;
			
			main::idleStreams() if !($chunkCount % 5);
		}

	}

	$request->addResult('count', $count);

	$request->setStatusDone();
}

#####################################################
# Slim Main artists Query
#####################################################	
sub mainArtistsQuery {
	my $request = shift;

	if (!Slim::Schema::hasLibrary()) {
		$request->setStatusNotDispatchable();
		return;
	}
	
	my $sqllog = main::DEBUGLOG && logger('database.sql');
	
	# get our parameters
	my $index         = $request->getParam('_index');
	my $quantity      = $request->getParam('_quantity');
		
	my $sql      = 'SELECT DISTINCT %s FROM albums ';
	my $c        = { 'albums.contributor' => 1 };
	my $w        = [];
	my $p        = [];
	my $order_by = "albums.contributor"; 
	my $limit;
	
		
	my $dbh = Slim::Schema->dbh;
	
	
	$sql .= "ORDER BY $order_by ";
	
	# Add selected columns
	my @cols = keys %{$c};
	$sql = sprintf $sql, join( ', ', map { $_ . " AS '" . $_ . "'" } @cols );
	
	my $stillScanning = Slim::Music::Import->stillScanning();
	
	# Get count of all results, the count is cached until the next rescan done event
	my $cacheKey = $sql . join( '', @{$p} );
	
	my $countsql = $sql;
	$countsql .= ' LIMIT ' . $limit if $limit;
	my ($count) = $cache->{$cacheKey} || $dbh->selectrow_array( qq{
		SELECT COUNT(*) FROM ( $countsql ) AS t1
	}, undef, @{$p} );
	
	if ( !$stillScanning ) {
		$cache->{$cacheKey} = $count;
	}

	if ($stillScanning) {
		$request->addResult('rescan', 1);
	}

	$count += 0;

	# now build the result
	my ($valid, $start, $end) = $request->normalize(scalar($index), scalar($quantity), $count);

	my $loopname = 'albums_loop';
	my $chunkCount = 0;

	if ($valid) {
		
		# Limit the real query
		if ($limit && !$quantity) {
			$quantity = "$limit";
			$index ||= "0";
		}
		if ( $index =~ /^\d+$/ && defined $quantity && $quantity =~ /^\d+$/ ) {
			$sql .= "LIMIT $index, $quantity ";
		}
		
		$log->debug("Album artists query: $sql / " . Data::Dump::dump($p));

		if ( main::DEBUGLOG && $sqllog->is_debug ) {
			$sqllog->debug( "Album artists query: $sql / " . Data::Dump::dump($p) );
		}

		my $sth = $dbh->prepare_cached($sql);
		$sth->execute( @{$p} );
		
		# Bind selected columns in order
		my $i = 1;
		for my $col ( @cols ) {
			$sth->bind_col( $i++, \$c->{$col} );
		}
		
		while ( $sth->fetch ) {
			
			utf8::decode( $c->{'albums.title'} ) if exists $c->{'albums.title'};
			utf8::decode( $c->{'contributors.name'} ) if exists $c->{'contributors.name'};
			
			$request->addResultLoopIfValueDefined($loopname, $chunkCount, 'artist_id', $c->{'albums.contributor'});				

			
			$chunkCount++;
			
			main::idleStreams() if !($chunkCount % 5);
		}
		
	}

	$request->addResult('count', $count);

	$request->setStatusDone();
}
#####################################################
# Slim Album artists Query

#Select distinct contributors.name 
#from contributors
#join albums on contributors.id = albums.contributor
#####################################################	
sub albumArtistsQuery {
	my $request = shift;

	if (!Slim::Schema::hasLibrary()) {
		$request->setStatusNotDispatchable();
		return;
	}
	
	my $sqllog = main::DEBUGLOG && logger('database.sql');
	
	# get our parameters
	my $index         = $request->getParam('_index');
	my $quantity      = $request->getParam('_quantity');
	my $tags    	  = $request->getParam('tags') || '';
	
		
	my $sql      = 'SELECT DISTINCT %s FROM contributors JOIN albums ON contributors.id = albums.contributor ';
	my $c        = { 'contributors.id' => 1,'contributors.name' => 1,'contributors.namesort' => 1 };
	my $w        = [];
	my $p        = [];
	my $order_by = "contributors.namesort"; 
	my $limit;
	
		
	my $dbh = Slim::Schema->dbh;
	my $collate = Slim::Utils::OSDetect->getOS()->sqlHelperClass()->collate();
	my $count_va = 0;
		
	my $indexList;
	if ($tags =~ /Z/) {
		my $pageSql = sprintf($sql, "SUBSTR(contributors.namesort,1,1), count(distinct contributors.id)")
			 . "GROUP BY SUBSTR(contributors.namesort,1,1) ORDER BY contributors.namesort $collate";
			 
		$indexList = $dbh->selectall_arrayref($pageSql, undef, @{$p});
		
		unshift @$indexList, ['#' => 1] if $indexList && $count_va;
		$request->addResult('indexList', $indexList) if $indexList;
		
		
		if ($tags =~ /ZZ/) {
			$request->setStatusDone();
			return
		}
	}
		
	# Add selected columns
	my @cols = keys %{$c};
	$sql = sprintf $sql, join( ', ', map { $_ . " AS '" . $_ . "'" } @cols );
	
	
	$sql .= "ORDER BY $order_by ";
	
	my $stillScanning = Slim::Music::Import->stillScanning();
	
	# Get count of all results, the count is cached until the next rescan done event
	my $cacheKey = $sql . join( '', @{$p} );
	
	my $countsql = $sql;
	$countsql .= ' LIMIT ' . $limit if $limit;
	
	$log->debug("Main artists count query: $sql / " . Data::Dump::dump($countsql));
	
	my ($count) = $cache->{$cacheKey} || $dbh->selectrow_array( qq{
		SELECT COUNT(*) FROM ( $countsql ) AS t1
	}, undef, @{$p} );
	
	if ( !$stillScanning ) {
		$cache->{$cacheKey} = $count;
	}

	if ($stillScanning) {
		$request->addResult('rescan', 1);
	}

	$count += 0;

	# now build the result
	my ($valid, $start, $end) = $request->normalize(scalar($index), scalar($quantity), $count);

	my $loopname = 'artists_loop';
	my $chunkCount = 0;

	if ($valid) {

		# We need to know the 'No album' name so that those items
		# which have been grouped together under it do not get the
		# album art of the first album.
		# It looks silly to go to Madonna->No album and see the
		# picture of '2 Unlimited'.
		my $noAlbumName = $request->string('NO_ALBUM');
		
		# Limit the real query
		if ($limit && !$quantity) {
			$quantity = "$limit";
			$index ||= "0";
		}
		if ( $index =~ /^\d+$/ && defined $quantity && $quantity =~ /^\d+$/ ) {
			$sql .= "LIMIT $index, $quantity ";
		}
		
		$log->debug("Main artists query: $sql / " . Data::Dump::dump($p));

		my $sth = $dbh->prepare_cached($sql);
		$sth->execute( @{$p} );
		
		# Bind selected columns in order
		my $i = 1;
		for my $col ( @cols ) {
			$sth->bind_col( $i++, \$c->{$col} );
		}
		
		while ( $sth->fetch ) {
			
			utf8::decode( $c->{'contributors.name'} ) if exists $c->{'contributors.name'};
			utf8::decode( $c->{'contributors.namesort'} ) if exists $c->{'contributors.namesort'};
			
						
			$request->addResultLoop($loopname, $chunkCount, 'id', $c->{'contributors.id'});
			$request->addResultLoop($loopname, $chunkCount, 'artist', $c->{'contributors.name'});	
			$request->addResultLoop($loopname, $chunkCount, 'sortName', $c->{'contributors.namesort'});			

			
			$chunkCount++;
			
			main::idleStreams() if !($chunkCount % 5);
		}

	}

	$request->addResult('count', $count);

	$request->setStatusDone();
}

#####################################################
# Slim Compilation artists Query

#Select distinct contributors.name 
#from contributors
#left join albums on contributors.id = albums.contributor
#join contributor_track on contributors.id = contributor_track.contributor
#where albums.contributor IS NULL AND contributor_track.role = 1

#####################################################	
sub compilationArtistsQuery {
	my $request = shift;

	if (!Slim::Schema::hasLibrary()) {
		$request->setStatusNotDispatchable();
		return;
	}
	
	my $sqllog = main::DEBUGLOG && logger('database.sql');
	
	# get our parameters
	my $index         = $request->getParam('_index');
	my $quantity      = $request->getParam('_quantity');
	my $tags    	  = $request->getParam('tags') || '';
	
		
	my $sql      = 'SELECT DISTINCT %s FROM contributors LEFT JOIN albums ON contributors.id = albums.contributor JOIN contributor_track on contributors.id = contributor_track.contributor';
	my $c        = { 'contributors.id' => 1,'contributors.name' => 1,'contributors.namesort' => 1 };
	my $w        = ['albums.contributor IS NULL','contributor_track.role = 1'];
	my $p        = [];
	my $order_by = "contributors.namesort"; 
	my $limit;
	
		
	my $dbh = Slim::Schema->dbh;
	

	if ( @{$w} ) {
		$sql .= ' WHERE ';
		$sql .= join( ' AND ', @{$w} );
		$sql .= ' ';
	}
	
	my $collate = Slim::Utils::OSDetect->getOS()->sqlHelperClass()->collate();
	my $count_va = 0;
		
	my $indexList;
	if ($tags =~ /Z/) {
		my $pageSql = sprintf($sql, "SUBSTR(contributors.namesort,1,1), count(distinct contributors.id)")
			 . "GROUP BY SUBSTR(contributors.namesort,1,1) ORDER BY contributors.namesort $collate";
			 
		$indexList = $dbh->selectall_arrayref($pageSql, undef, @{$p});
		
		unshift @$indexList, ['#' => 1] if $indexList && $count_va;
		$request->addResult('indexList', $indexList) if $indexList;
		
		
		if ($tags =~ /ZZ/) {
			$request->setStatusDone();
			return
		}
	}
			
	# Add selected columns
	my @cols = keys %{$c};
	$sql = sprintf $sql, join( ', ', map { $_ . " AS '" . $_ . "'" } @cols );
	
	$sql .= "ORDER BY $order_by ";
	my $stillScanning = Slim::Music::Import->stillScanning();
	
	# Get count of all results, the count is cached until the next rescan done event
	my $cacheKey = $sql . join( '', @{$p} );
	
	my $countsql = $sql;
	$countsql .= ' LIMIT ' . $limit if $limit;
	my ($count) = $cache->{$cacheKey} || $dbh->selectrow_array( qq{
		SELECT COUNT(*) FROM ( $countsql ) AS t1
	}, undef, @{$p} );
	
	if ( !$stillScanning ) {
		$cache->{$cacheKey} = $count;
	}

	if ($stillScanning) {
		$request->addResult('rescan', 1);
	}

	$count += 0;

	# now build the result
	my ($valid, $start, $end) = $request->normalize(scalar($index), scalar($quantity), $count);

	my $loopname = 'artists_loop';
	my $chunkCount = 0;

	if ($valid) {

		# We need to know the 'No album' name so that those items
		# which have been grouped together under it do not get the
		# album art of the first album.
		# It looks silly to go to Madonna->No album and see the
		# picture of '2 Unlimited'.
		my $noAlbumName = $request->string('NO_ALBUM');
		
		# Limit the real query
		if ($limit && !$quantity) {
			$quantity = "$limit";
			$index ||= "0";
		}
		if ( $index =~ /^\d+$/ && defined $quantity && $quantity =~ /^\d+$/ ) {
			$sql .= "LIMIT $index, $quantity ";
		}
		
		$log->debug("Compilation artists query: $sql / " . Data::Dump::dump($p));

		if ( main::DEBUGLOG && $sqllog->is_debug ) {
			$sqllog->debug( "Compilation artists query: $sql / " . Data::Dump::dump($p) );
		}

		my $sth = $dbh->prepare_cached($sql);
		$sth->execute( @{$p} );
		
		# Bind selected columns in order
		my $i = 1;
		for my $col ( @cols ) {
			$sth->bind_col( $i++, \$c->{$col} );
		}
		
		while ( $sth->fetch ) {
			
			utf8::decode( $c->{'contributors.name'} ) if exists $c->{'contributors.name'};
			utf8::decode( $c->{'contributors.namesort'} ) if exists $c->{'contributors.namesort'};
			
						
			$request->addResultLoop($loopname, $chunkCount, 'id', $c->{'contributors.id'});
			$request->addResultLoop($loopname, $chunkCount, 'artist', $c->{'contributors.name'});	
			$request->addResultLoop($loopname, $chunkCount, 'sortName', $c->{'contributors.namesort'});			

			
			$chunkCount++;
			
			main::idleStreams() if !($chunkCount % 5);
		}

	}

	$request->addResult('count', $count);

	$request->setStatusDone();
}
#####################################################
# Return the icon url for an favorite item
#####################################################
sub iconForUrl {
	
	my $request = shift;
	my $url  = $request->getParam('_url');
		
	$log->debug("Favorite url :".$url);
		
	my $loopname = 'url_loop';
	my $cnt = 0;	
	
	my $coverArtUrl = Slim::Player::ProtocolHandlers->iconForURL($url) || 'html/images/favorites.png';
		
	$request->addResult('coverArtUrl',$coverArtUrl);	
		
	$request->setStatusDone();   
	
}

#####################################################
# Return a list of images in album folder
#####################################################
sub imagelist {
	myDebug("imageList");
	my $request = shift;

	my $client = $request->client();
	my $albumId  = $request->getParam('_albumId');
	
	$log->debug("albumId :".$albumId);
	
	if (Slim::Music::Import->stillScanning()) {
		$request->addResult("rescan", 1);
	}
	
	my $loopname = 'images_loop';
	my $cnt = 0;
		
	my @images = searchAlbumImages($albumId);
        
     foreach my $image (@images)
	{
	  $request->addResult('images',$image);
      $cnt++;
	}   
        
     $request->setStatusDone();   

}

#####################################################
# Return a album track index list
#####################################################
sub albumIndexList {
	myDebug("imageList");
	my $request = shift;

	my $client = $request->client();
	my $albumId  = $request->getParam('_albumId');
	
	if (Slim::Music::Import->stillScanning()) {
		$request->addResult("rescan", 1);
	}
	
	#
	#
	my $result = getTracksByAlbum($albumId);
	my $tracks = ${$result}{tracks_loop};
	
	my %tracks_index = ();
		
	for (my $i=0; $i < ${$result}{count}; $i++) {
		my $discNum = ${$tracks}[$i]{disc};
		
		if ( exists $tracks_index{$discNum}) {
			my $count = $tracks_index{$discNum};
			$tracks_index{$discNum} = $count + 1;
		}
		else {
			$tracks_index{$discNum} = 1;
		}
		
	}
		
	my $loopname = 'index_loop';
	my $cnt = 0;
	
	for (my $i = 1; $i <= keys(%tracks_index); $i++) {
		$request->addResultLoop($loopname, $cnt, 'Disc '.$i, $tracks_index{$i});
		$cnt++;
	}
				
     $request->addResult('count', $cnt);    
     $request->setStatusDone();   

}

sub searchAlbumImages() {
	my $albumId = shift;
	my @images = ();
	my @fileNames = ();
	
	my $albumFolder = decodeFileUrl(getAlbumFolders($albumId));
	
	$log->debug("Album Folder :".$albumFolder);
		
		
	my @files = File::Find::Rule->file()->name( "*.jpg","*.png","*.jpeg","*.JPG","*.PNG","*.pdf","*.PDF")->in($albumFolder);
	
	foreach my $file (@files) {
		
		
		# fixe file name
		$file = canonpath($file);
		
		$log->debug("Image File :".$file);
		
		
		my ($name,$path,$suffix) = fileparse($file, qr/\.[^.]*/);

			
		$log->debug("path :".$path);
			
		my $directory_separator = canonpath "/";
		my $substring = $albumFolder.$directory_separator;
			
			
		#escape back slashes
		$substring =~ s/\\/\\\\/g;
		
		$log->debug("substring before mods  :".$substring);	
			
		#first remove brackets from path if any because the string substitution does not work otherwise
		$path =~ s![()]!!g;
		$path =~ s![\[\]]!!g;
			
		#remove brackets from substring  if any because the string substitution does not work otherwise
		$substring =~ s![()]!!g;
		$substring =~ s![\[\]]!!g;
		
		#first remove + from path if any because the string substitution does not work otherwise
		$path =~ s![+]!!g;
			
		#remove + from substring  if any because the string substitution does not work otherwise
		$substring =~ s![+]!!g;
			
		$log->debug("path :".$path);
			
		$log->debug("substring to remove :".$substring);
			
		#remove albumFolder from path
		$path =~ s!$substring!!g;
			
		$log->debug("path after :".$path);
		
		
		#check if file name duplicate else insert in images array
		if ($name.$suffix ~~ @fileNames  ) {
			
			$log->debug("Duplicate Image : ".$name.$suffix);
		}
		else {
			$log->debug("Adding Image : ".$name.$suffix);
  			push(@fileNames,$name.$suffix);
  			push(@images,$path.$name.$suffix);
		}
				
	}
		
	return \@images;
}

#####################################################
# confirm serving of videos
#####################################################
sub canServeVideo {

	my $request = shift;
        
    $request->setStatusDone();   

}

sub handleFile {
	my ($client, $params, $callback, $httpClient, $httpResponse, $request) = @_;
		
	$log->debug("Handling file Path :".$params->{path});
	
	my $path = $params->{path};
	my $albumFolder = decodeFileUrl(getAlbumFolders(getAlbumId($path)));
	
	my $directory_separator = canonpath "/";
	
	my $file =$albumFolder.$directory_separator.getFileName($path);
		
	$log->debug("File : ".$file);
	
	my ($name,$path,$suffix) = fileparse($file, qr/\.[^.]*/);

	my $content = do {
		local $/ = undef;
		open my $fh, "<", $file or $log->error("could not open $file: $!");
		if( $fh) {
			binmode $fh;
			<$fh>;
		}
	};

	$httpResponse->header( 'Content-Length' => length($content) );
	if( $suffix =~ /.png/i ) {
		$httpResponse->header( 'Content-Type' => "image/png" );
	} else {
		$httpResponse->header( 'Content-Type' => "image/jpg" );
	}
	return \$content;
}


sub handleGetInfo() {
	
	my ($client, $params, $callback, $httpClient, $httpResponse, $request) = @_;
			
	$log->debug("Getting Info");
				
	my $path = $params->{path};
	
	$log->debug("Path : ".$path);
	my $albumFolder = decodeFileUrl(getAlbumFolders(getAlbumId($path)));
	
	$log->debug("AlbumFolder : ".$albumFolder);
	
	my @files = File::Find::Rule->file()->name( qr/.*info.*\.txt$/i )->in($albumFolder);
	
	my $count = @files;
	
	if (($count  ==  0)) {
		@files = File::Find::Rule->file()->name( qr/.*\.txt$/i )->in($albumFolder);
		$count = @files;
	}
	
	if ($count  >  0) {
		
		my $file =$files[0];
		
		$log->debug("File : ".$file);
	
	
		my ($name,$path,$suffix) = fileparse($file, qr/\.[^.]*/);

		my $content = do {
			local $/ = undef;
			open my $fh, "<:encoding(UTF-8)", $file or $log->error("could not open $file: $!");
			if( $fh) {
				<$fh>;
			}
		};

	$httpResponse->header( 'Content-Length' => length($content) );
	$httpResponse->header( 'Content-Type' => "text/plain" );
		
	return \$content;
		
	}
	else {
	
	my $content = "File not Found";
	
	$httpResponse->code(RC_NOT_FOUND);
	$httpResponse->header( 'Content-Length' => length($content) );
	$httpResponse->header( 'Content-Type' => "text/plain" );
		
	return \$content;
	}

	
	
}

sub getAlbumId() {
	
	my $path =  shift @_;
	my @parts = split(/\//,$path);
	
	return @parts[2];
}

sub getFileName() {
	
	my $path =  shift @_;
	my @parts = split(/\//,$path);
		
	my $albumId = @parts[2];
	
	my $pos = index($path,$albumId);
		
	return substr($path,$pos + length($albumId) + 1);
	
}

sub getVideoId() {
	
	my $path =  shift @_;
	my @parts = split(/\//,$path);
	
	return @parts[3];
}

sub decodeUrl() {
	
	my $string = shift @_;
		
	$string =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
	
	return $string;
}

sub decodeFileUrl() {
	
	my $string = shift @_;
	
	$log->debug("File Url :".$string);
		
	my $file = URI->new($string)->file;
	
	$file =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
	
	return $file;
}

#
# Find root folder for album tracks
#
sub getAlbumFolders( ) {
		
	my $albumId = shift @_;
	
	my @albumFolders = ();
	
	my $result = getTracksByAlbum($albumId);
	my $tracks = ${$result}{tracks_loop};
	
	my @pathList = ();
		
	for (my $i=0; $i < ${$result}{count}; $i++) {
		#$log->debug("Directory : ".dirname( ${$tracks}[$i]{url}));
		push @pathList,dirname( ${$tracks}[$i]{url});
	}
	
	my %pathHash = map {$_,1} @pathList;
	
	my $directory_separator =  "/";
		
	return common_prefix($directory_separator, keys %pathHash);
}

sub getTracksByAlbum {
		my ($albumid, $sort) = @_;
		my @tracks = ();

		my $orderBy = { -asc => 'me.disc,tracknum'};
		if( $sort) {
			$orderBy = getTrackOrderBy( $sort);
		}
		
		my @trackRS = Slim::Schema->rs('Track')->search( {'album' => $albumid }, {prefetch => [], 'distinct' => 1, order_by => $orderBy} );
		if(@trackRS) {
			foreach (@trackRS) {
				push @tracks, resolveTrackJ( $_);
			}
		}
				
		my $res = {
			tracks_loop => \@tracks,
			count => scalar(@tracks)
		};
		
		return $res;
}


sub getVideoById {
	my $videoHash = shift;
	my $cache = {};

	my $sqllog = main::DEBUGLOG && logger('database.sql');
	
	my $collate = Slim::Utils::OSDetect->getOS()->sqlHelperClass()->collate();
	
	my $sql      = 'SELECT %s FROM videos ';
	my $c        = { 'hash' => 1, 'titlesearch' => 1, 'titlesort' => 1,'url' =>1 };
	my $w        = [];
	my $p        = [];
	my $order_by = "videos.titlesort $collate";
	my $limit;
	
	# Normalize and add any search parameters
	if ( defined $videoHash ) {
		push @{$w}, 'videos.hash = ?';
		push @{$p}, $videoHash;
	}

	if ( @{$w} ) {
		$sql .= 'WHERE ';
		$sql .= join( ' AND ', @{$w} );
		$sql .= ' ';
	}
	
	# Add selected columns
	my @cols = keys %{$c};
	$sql = sprintf $sql, join( ', ', map { 'videos.'.$_ . " AS '" . $_ . "'" } @cols );
	
	my $stillScanning = Slim::Music::Import->stillScanning();
	
	my $dbh = Slim::Schema->dbh;
	
	# Get count of all results, the count is cached until the next rescan done event
	my $cacheKey = $sql . join( '', @{$p} );
	
	my ($count) = $cache->{$cacheKey} || $dbh->selectrow_array( qq{
		SELECT COUNT(*) FROM ( $sql ) AS t1
	}, undef, @{$p} );
	
	if ( !$stillScanning ) {
		$cache->{$cacheKey} = $count;
	}

	$count += 0;

	my $totalCount = $count;
	
	# execute query
	my $sth = $dbh->prepare_cached($sql);
	$sth->execute( @{$p} );
	
	# Bind selected columns in order
	my $i = 1;
	for my $col ( @cols ) {
		$sth->bind_col( $i++, \$c->{$col} );
	}
	
	$sth->fetch;
	
	utf8::decode( $c->{'videos.title'} ) if exists $c->{'videos.title'};
	utf8::decode( $c->{'videos.album'} ) if exists $c->{'videos.album'};
	
	
	return $c;
}


# process the tracks result set
sub resolveTrackJ {
	my $t = shift;
	my $noExtras = shift;
	my @atts = ('id', 'name', 'albumid', 'duration', 'prettyBitRate', 
				'tracknum', 'year', 'disc', 'filesize', 'url', 'replay_gain','added_time', 'updated_time', 'tagversion', 'timestamp', 'coverid','lyrics');
	my @atts_e = ('comment');

	if (! $noExtras) {
		push @atts,@atts_e;
	}

	my $r = {};
	foreach my $a (@atts) {
		$r->{$a} = $t->$a;
	}

	return $r;
}

#
# find root folder
#
sub common_prefix {    
    my $sep = shift;
    my $paths = join "\0", map { $_.$sep } @_;
    $paths =~ /^ ( [^\0]* ) $sep [^\0]* (?: \0 \1 $sep [^\0]* )* $/sx;
    return $1;
}

# Always end with a 1 to make Perl happy
1;
