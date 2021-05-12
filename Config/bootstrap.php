<?php

Configure::write('XcdbsyncJob.bootstrap', true);
Configure::write('XcdbsyncJob.xdcdb.userinfo.url.base', 'https://xsede-xdcdb-api.xsede.org/userinfo/v1/people/by_username/portal.teragrid/');
Configure::write('XcdbsyncJob.xdcdb.apiresource', 'registry.xsede.org');
Configure::write('XcdbsyncJob.xdcdb.apikey', 'XXXXXXXX');
