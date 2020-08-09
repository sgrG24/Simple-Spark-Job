import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

public class CustomCredentialProvider implements AWSCredentialsProvider, Closeable {

    private AWSSecurityTokenService stsClient;
    private final String sessionName;
    private final long duration;
    private final String arn;
    private final TimeProvider timeProvider;
    private final Regions clientRegion = Regions.US_EAST_1;

    private static final int DURATION_TIME_SECONDS = 3600;
    private static final int EXPIRY_TIME_MILLIS = 60 * 1000;

    private final Logger logger = LogManager.getLogger(CustomCredentialProvider.class);
    private AWSSessionCredentials sessionCredentials;
    private Date sessionCredentialsExpiration;




    public CustomCredentialProvider(URI uri, Configuration conf) throws IOException {
        this.arn = conf.getTrimmed("fs.s3a.assumed.role.arn", "");

        if (StringUtils.isEmpty(this.arn)) {
            throw new IOException("Unset property fs.s3a.assumed.role.arn");
        } else {
            this.timeProvider = new TimeProvider() {
                @Override
                public long currentTimeMillis() {
                    return System.currentTimeMillis();
                }
            };

            this.duration = DURATION_TIME_SECONDS;
            this.sessionName = conf.getTrimmed("fs.s3a.assumed.role.session.name", buildSessionName());
            this.stsClient =  AWSSecurityTokenServiceClientBuilder
                    .standard()
                    .withRegion(clientRegion)
                    .build();

        }

    }

    private String buildSessionName() {
        String sessionName = "custom-credential-provider" + this.timeProvider.currentTimeMillis();
        return sessionName;
    }

    @Override
    public AWSCredentials getCredentials() {
        this.logger.debug("get credential called");

        if(this.arn == null) {
            this.logger.warn("assume role not provided");
            return null;
        }

        if (needsNewSession()) {
            startSession();
        }
        return sessionCredentials;
    }

    private AWSCredentials startSession() {
        try {
            String sessionName = "custom-credential-provider" + this.timeProvider.currentTimeMillis();
            AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
                    .withRoleArn(this.arn)
                    .withRoleSessionName(sessionName)
                    .withDurationSeconds(DURATION_TIME_SECONDS);
            Credentials stsCredentials = stsClient.assumeRole(assumeRoleRequest).getCredentials();
            sessionCredentials = new BasicSessionCredentials(stsCredentials.getAccessKeyId(),
                    stsCredentials.getSecretAccessKey(), stsCredentials.getSessionToken());
            sessionCredentialsExpiration = stsCredentials.getExpiration();
        }
        catch (Exception ex) {
            logger.warn("Unable to start a new session. Will use old session credential or fallback credential", ex);
        }

        return sessionCredentials;
    }

    private boolean needsNewSession() {
        if (sessionCredentials == null) {
            // Increased log level from debug to warn
            logger.warn("Session credentials do not exist. Needs new session");
            return true;
        }

        long timeRemaining = sessionCredentialsExpiration.getTime() - timeProvider.currentTimeMillis();
        if(timeRemaining < EXPIRY_TIME_MILLIS) {
            // Increased log level from debug to warn
            logger.warn("Session credential exist but expired. Needs new session");
            return true;
        } else {
            // Increased log level from debug to warn
            logger.warn("Session credential exist and not expired. No need to create new session");
            return false;
        }
    }

    @Override
    public void refresh() {
        logger.debug("refresh called");
        startSession();
    }

    @Override
    public void close() throws IOException {

    }
}
