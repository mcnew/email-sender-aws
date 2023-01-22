import { CodeCommitClient, GetFileCommand } from '@aws-sdk/client-codecommit';
import { SendEmailCommand, SESClient } from '@aws-sdk/client-ses';
import * as Handlebars from 'handlebars';
import { Context, SQSEvent, SQSRecord, SQSBatchResponse } from 'aws-lambda';
import { nextTick } from 'process';

const BRANCH = process.env.BRANCH;
const REPOSITORY = process.env.REPOSITORY;
const DISABLE_CACHE = process.env.DISABLE_CACHE;
const SOURCE_EMAIL = process.env.SOURCE_EMAIL;

const _TEMPLATES: { [key: string]: HandlebarsTemplateDelegate<any> } = {};
let configured = false;

let codeCommit = new CodeCommitClient({
    apiVersion: "2015-04-13"
});
let ses = new SESClient({
    apiVersion: "2010-12-01"
});

async function getTextFile(name: string) {
    let command = new GetFileCommand({
        commitSpecifier: BRANCH,
        filePath: name,
        repositoryName: REPOSITORY
    });
    return codeCommit.send(command)
        .then(resp => {
            let fileContent = resp.fileContent;
            if (fileContent) {
                return Buffer.from(fileContent).toString('utf8');
            } else {
                return undefined;
            }
        });
}

async function getGlobalConfiguration() {
    return getTextFile('configuration.json').then(text => {
        if (text) {
            return JSON.parse(text);
        } else {
            return {};
        }
    });
}

async function _getTemplate(name: string) {
    return getTextFile(`email/${name}.hbs`).then(text => {
        return Handlebars.compile(text);
    });
}

async function getTemplate(name: string) {
    if (DISABLE_CACHE === 'true') {
        return _getTemplate(name);
    } else {
        let template = _TEMPLATES[name];

        if (template === undefined) {
            return _getTemplate(name).then(tmpl => {
                _TEMPLATES[name] = tmpl;
                return tmpl;
            });
        } else {
            return template;
        }
    }
}

async function _loadPartials(partials: [{ source: string, name: string }], ndx: number): Promise<{partial: string, error?: string}[]> {
    if (partials.length === ndx) {
        return [];
    } else {
        let partial = partials[ndx];
        var original = partial.source;
        if (original.startsWith('/') || original.startsWith('.')) {
            let next = await _loadPartials(partials, ndx + 1);
            next.unshift({
                partial: partial.name,
                error: 'invalid partial'
            });
            return next;
        } else {
            let source = `/partial/${partial.source}.hbs`
            console.log(`load partial: ${source}`);
            return getTextFile(source).then(async (text): Promise<{ partial: string, error?: string }[]> => {
                let current;
                if (text) {
                    Handlebars.registerPartial(partial.name, text);
                    current = {
                        partial: partial.name,
                    };
                } else {
                    current = {
                        partial: partial.name,
                        error: 'empty or not found'
                    };
                }
                let next = await _loadPartials(partials, ndx + 1);
                next.unshift(current);
                return next;
            });
        }
    }
}

async function loadPartials(partials: [{ source: string, name: string }]) {
    return _loadPartials(partials, 0);
}

async function buildBody(templateName: string, data: { any: any }) {
    return getTemplate(templateName)
            .then(template => template(data));
}

async function sendEmail(to: string, subject: string, body: string) {
    let command = new SendEmailCommand({
        Destination: {
            ToAddresses: [
                to
            ]
        },
        Message: {
            Body: {
                Html: {
                    Charset: "UTF-8",
                    Data: body
                }
            },
            Subject: {
                Charset: "UTF-8",
                Data: subject
            }
        },
        Source: SOURCE_EMAIL
    });
    ses.send(command);
}

async function _processMessages(messages: SQSRecord[], ndx: number): Promise<{ error?: string, messageId: string }[]> {
    if (ndx === messages.length) {
        return [];
    } else {
        let message = messages[ndx];
        let body = JSON.parse(message.body);
        let messageId = message.messageId;
        let receiptHandle = message.receiptHandle;

        let templateName = body.template;

        if (templateName.charAt(0) === '.' || templateName.charAt(0) === '/') {
            console.warn(`invalid template(${messageId}): ${templateName}`)
            let next = await _processMessages(messages, ndx + 1);
            next.unshift({ messageId: messageId, error: `invalid template(${messageId}): ${templateName}` });
            return next;
        } else {
            let templateContext = body.data;
            let subject = body.subject;
            let to = body.addresse;

            return buildBody(templateName, templateContext)
                .then(async (body): Promise<{ error?: string, messageId: string }[]> => {
                    let nextProm = _processMessages(messages, ndx + 1);
                    let current = await sendEmail(to, subject, body);
                    let next = await nextProm;
                    next.unshift({ messageId: messageId });
                    return next;
                });
        }
    }
}

async function processMessages(messages: SQSRecord[]) {
    return _processMessages(messages, 0);
}

exports.handler = async (event: SQSEvent, context: Context): Promise<SQSBatchResponse> => {
    let result;
    if (configured) {
        console.info('already configured');
        result = await processMessages(event.Records);
    } else {
        console.info('load config ...');
        result = await getGlobalConfiguration().then(async (data) => {
            console.info('load partials ...');
            return loadPartials(data.partials).then(() => {
                configured = true;
                return processMessages(event.Records);
            });
        });
    }

    return {
        batchItemFailures: result
            .filter(e => e.error !== '')
            .map(e => {
                return {
                    itemIdentifier: e.messageId
                };
            })
    }
}
