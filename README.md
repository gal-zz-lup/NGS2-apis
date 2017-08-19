# NGS2-APIs
This repository contains applications to handle various tasks through publicly available APIs, utilizing custom wrappers and SDKs.

## Payment processing with PayPal

## Text Messages with Twillo
### Introduction
As part of World Labs, Gallup will be sending participants SMS reminders before their experiments via [Twilio](https://www.twilio.com) APIs. This allows flexibility in programming reminders and ensuring participant participation.

### Setup
Specifically, this repository uses the [Twilio SDK for Python](https://github.com/twilio/twilio-python) to wrap up messages for delivery. To execute these calls, a few pieces of information are necessary:
1. A .csv file with the numbers to send messages to.
2. A .txt file with the message text to be sent.
3. REST API keys to authenticate against the API.

#### Content details
##### Phone numbers
The .csv containing phone numbers to send messages to should be structured as follows:
* `ExternalDataReference`: A alphanumeric field with the participant's unique identifying number.
* `SMS_PHONE_CLEAN`: A numeric field with the participant's phone number (without country code).

##### Message text
The .txt file should contain nothing but the text that is to be sent to participants. For example, the file could be a .txt that consisted of just the following: `This is the message to send.`

#### Running the program
The processing program is written in Python and can be called from the command line. It takes four required arguments:
* `-a` or `--auth`: **This argument requires three inputs.** The first is the Twilio REST API key for the account and the second is the Twilio REST API secret. Finally, the third is the sending phone number associated with the Twilio account. These are all assigned/designated once signing up for a Twilio developer account.
* `-c` or `--content`: This argument is a .txt file with the text be to sent by SMS.
* `-n` or `--nation`: This indicates to which country the SMS messages will be sent. Currently, only implemented for the United States (enter `US`). `
* `-p` or `--phones`: This argument is a .csv fule with the phone number and participant IDs to whom SMS messages will be sent.

#### Command-line Execution
To execute the program, below is an example call:

```
$ python messaging/sms.py -a $TWILIO_ID $TWILIO_SECRET $TWILIO_PHONE \
                          -c ~/Documents/ngs2/message_text.txt \
                          -n US \
                          -p ~/Documents/ngs2/sms_phones.csv
```

where `$TWILIO_ID`, `$TWILIO_SECRET`, and `$TWILIO_PHONE` are stored environmental variables with the appropriate REST API key/secret values.

#### Logging
The messaging program is set up with logging. Logging is important in being able to have a record of each time the program is run and what actually happened during the execution. The logging file is called `twilio_processing.log` and stored in the `messaging` folder of the repository. Each execution of the program will *append* to the log, not overwrite the last transaction, meaning a complete record of all executions is possible to have on disk.

#### Testing
This program includes a set of tests for the various functions that are being called through execution. They should be kept up-to-date as program functions change.

### Troubleshooting
If there are questions or problems, contact [Matt Hoover](matt_hoover@gallup.com) for assistance.

### Conclusion
This messaging program is a simple wrapper to send SMS messages using the Twilio APIs and SDK. If needed, it can be expanded upon and used for other means.
