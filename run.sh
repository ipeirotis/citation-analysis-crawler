#!/bin/bash -e
# Runs the wcrawler.

function redtext { echo $'\x1b[01;31m\x1b[K'${@:1}$'\x1b[m\x1b[K'; }

function message () {
    redtext ${@:1}
}

function mysleep() {
    message Will now sleep for "$1" seconds...
    sleep "$1"
}

err_report() {
    message "Error on line $1 of file $2"
    message "Command:"
    message "$BASH_COMMAND"

    notify_error "$1" "$2" "$BASH_COMMAND"
}

trap 'err_report $LINENO $0' ERR

function notify_blocked() {
    message notifying supervisor: blocked
    wget "$SUPERVISOR_BASE_URL"/blocked -O /dev/null
}

function notify_got_cookie() {
    message notifying supervisor: got_cookie
    wget "$SUPERVISOR_BASE_URL"/got-cookie -O /dev/null
}

function notify_crawled() {
    message notifying supervisor: crawled "$1"
    scholar_id="$1"
    wget "$SUPERVISOR_BASE_URL"/crawled/"${scholar_id}" -O /dev/null
}

function notify_crawled_author() {
    message notifying supervisor: crawled_author "$1"
    scholar_id="$1"
    wget "$SUPERVISOR_BASE_URL"/crawled-author/"${scholar_id}" -O /dev/null
}

function notify_added_to_db() {
    message notifying supervisor: added_to_db "$1"
    scholar_id="$1"
    wget "$SUPERVISOR_BASE_URL"/added-to-db/"${scholar_id}" -O /dev/null
}

function notify_added_author_to_db() {
    message notifying supervisor: added_author_to_db "$1"
    scholar_id="$1"
    wget "$SUPERVISOR_BASE_URL"/added-author-to-db/"${scholar_id}" -O /dev/null
}

function notify_crawling_failure_author() {
    message notifying supervisor: crawling_failure_author "$1" "$2"
    scholar_id="$1"
    page="$2"
    reason="$3"
    wget "$SUPERVISOR_BASE_URL"/crawling-failure-author/"${scholar_id}"/"${page}"/"${reason}" -O /dev/null
}

function notify_got_author_canonical_id() {
    message notifying supervisor: got_author_canonical_id "$1" "$2"
    previous="$1"
    canonical="$2"
    wget "$SUPERVISOR_BASE_URL"/got-author-canonical-id/"${previous}"/"${canonical}" -O /dev/null
}

function notify_started() {
    message notifying supervisor: started
    wget "$SUPERVISOR_BASE_URL"/started -O /dev/null
}

function notify_got_stale_publications() {
    message notifying supervisor: got_stale_publications "$1"
    count="$1"
    wget "$SUPERVISOR_BASE_URL"/got-stale-publications/"${count}" -O /dev/null
}

function notify_got_stale_authors() {
    message notifying supervisor: got_stale_authors "$1"
    count="$1"
    wget "$SUPERVISOR_BASE_URL"/got-stale-authors/"${count}" -O /dev/null
}

function notify_got_requested_publication() {
    message notifying supervisor: got_requested_publication "$1"
    scholar_id="$1"
    wget "$SUPERVISOR_BASE_URL"/got-requested-publication/"${scholar_id}" -O /dev/null
}

function notify_got_requested_author() {
    message notifying supervisor: got_requested_author "$1"
    scholar_id="$1"
    wget "$SUPERVISOR_BASE_URL"/got-requested-author/"${scholar_id}" -O /dev/null
}

function notify_crawling_started() {
    message notifying supervisor: crawling_started
    wget "$SUPERVISOR_BASE_URL"/crawling-started -O /dev/null
}

function notify_exited() {
    message notifying supervisor: exited
    wget "$SUPERVISOR_BASE_URL"/exited -O /dev/null
}

function notify_got_ip() {
    message notifying supervisor: got_ip "$1"
    ip="$1"
    wget "$SUPERVISOR_BASE_URL"/got-ip/"${ip}" -O /dev/null
}

function poll_command() {
    message polling supervisor for command
    message=$(wget -q "${SUPERVISOR_BASE_URL}"/poll-command -O -)
    read COMMAND ARGUMENT1 ARGUMENT2 <<<"${message}"
}

function notify_error() {
    message notifying supervisor: error
    bash_lineno="$1"
    bash_filename="$2"
    bash_command="$3"

    wget --post-data "LINE=${bash_lineno}&FILENAME=${bash_filename}&COMMAND=${bash_command}" "$SUPERVISOR_BASE_URL"/error -O /dev/null
}

function got_blocked_by_login() {
    gzipped_response="$1"
    gunzip -c "${gzipped_response}" | grep -q "if you are not redirected within a few seconds"
}

function got_blocked_by_404() {
    gzipped_response="$1"
    gunzip -c "${gzipped_response}" | grep -q "no content found for this URL"
}

function crawl_publication() {
    SCHOLAR_ID="$1"

    URL="https://scholar.google.com/citations?view_op=view_citation&citation_for_view=${SCHOLAR_ID}&hl=en"

    while true
    do
        if [ ! -f cookies.txt ]
        then
            wget -d -S --header="Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" --header="Accept-Language: en-US,en;q=0.5" --header="Accept-Encoding: gzip, deflate, br" --delete-after "${URL}"  || { notify_blocked; mysleep 600; continue; }

            # Get cookie from https://scholar.google.com/scholar/images/cleardot.gif.
            wget -d -S --header="Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" --header="Accept-Language: en-US,en;q=0.5" --header="Accept-Encoding: gzip, deflate, br" --header="Referer: ${URL}"  --keep-session-cookies --save-cookies=cookies.txt --delete-after 'https://scholar.google.com/scholar/images/cleardot.gif' || { notify_blocked; mysleep 600; continue; }

            notify_got_cookie
        fi

        wget -d -S --header="Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" --header="Accept-Language: en-US,en;q=0.5" --header="Accept-Encoding: gzip, deflate, br" --load-cookies=cookies.txt --keep-session-cookies --save-cookies=cookies.txt "${URL}" -O publications_cache/"${SCHOLAR_ID}".html.gz || { notify_blocked; mysleep 600; continue; }

        break
    done

    notify_crawled "${SCHOLAR_ID}"
    python parse_and_add_to_db.py "${SCHOLAR_ID}"
    notify_added_to_db "${SCHOLAR_ID}"
}

function get() {
    MY_URL="$1"
    message Getting "${MY_URL}"
    wget -d -S --header="Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" --header="Accept-Language: en-US,en;q=0.5" --header="Accept-Encoding: gzip, deflate, br" "${@:2}" "${MY_URL}"
}


function crawl_author() {
    SCHOLAR_ID="$1"
    REAL_SCHOLAR_ID="${SCHOLAR_ID}"

    COUNT=20
    START=0

    CACHE_DIR=authors_cache

    GOT_HISTOGRAM=0
    GOT_COLLEAGUES=0

    FAIL_404=

    while true
    do
        # list_works URL
        URL="https://scholar.google.com/citations?view_op=list_works&pagesize=${COUNT}&cstart=${START}&sortby=pubdate&hl=en&user=${REAL_SCHOLAR_ID}"

        if [ ! -f cookies.txt ]
        then
            FOR_COOKIES="${CACHE_DIR}"/"${SCHOLAR_ID}".html.gz

            get "${URL}" --keep-session-cookies --save-cookies=cookies.txt -O "${FOR_COOKIES}"  || { notify_blocked; mysleep 600; continue; }

            # We can be blocked by a page that asks us to log in.
            got_blocked_by_login "${FOR_COOKIES}" && { message Got blocked by login screen; notify_blocked; mysleep 600; continue; }
            # Or by a 404 page:
            got_blocked_by_404 "${FOR_COOKIES}" && { message Got blocked by 404 page; notify_blocked; FAIL_404=main; break; }


            REAL_SCHOLAR_ID="$(python get_real_scholar_id.py ${CACHE_DIR} ${SCHOLAR_ID})"

            message "Given scholar_id was ${SCHOLAR_ID}, real one is ${REAL_SCHOLAR_ID}"

            # Get cookie from https://scholar.google.com/intl/en/scholar/images/1x/scholar_logo_30dp.png

            get 'https://scholar.google.com/intl/en/scholar/images/1x/scholar_logo_30dp.png' --header="Referer: ${URL}" --load-cookies=cookies.txt   --keep-session-cookies --save-cookies=cookies.txt --delete-after || { notify_blocked; mysleep 600; continue; }

            notify_got_cookie

            message Done with cookies #, will now wait

            # mysleep $((2 * ${QUERY_INTERVAL} ))
        else
            # Get the first page to find out about a renaming, or get an early 404.
            FOR_CANONICAL_ID="${CACHE_DIR}"/"${SCHOLAR_ID}".html.gz

            get "${URL}" --keep-session-cookies --save-cookies=cookies.txt -O "${FOR_CANONICAL_ID}"  || { notify_blocked; mysleep 600; continue; }

            # We can be blocked by a page that asks us to log in.
            got_blocked_by_login "${FOR_CANONICAL_ID}" && { message Got blocked by login screen; notify_blocked; mysleep 600; continue; }
            # Or by a 404 page:
            got_blocked_by_404 "${FOR_CANONICAL_ID}" && { message Got blocked by 404 page; notify_blocked; FAIL_404=main; break; }


            REAL_SCHOLAR_ID="$(python get_real_scholar_id.py ${CACHE_DIR} ${SCHOLAR_ID})"

            message "Given scholar_id was ${SCHOLAR_ID}, real one is ${REAL_SCHOLAR_ID}"
            if [ -n "${REAL_SCHOLAR_ID}" -a  "${SCHOLAR_ID}" != "${REAL_SCHOLAR_ID}" ]
            then
                notify_got_author_canonical_id "${SCHOLAR_ID}" "${REAL_SCHOLAR_ID}"
            fi

        fi

        if [ "${GOT_HISTOGRAM}" -eq 0 ]
        then
            # Crawl citation histogram page.
            message "Getting histogram page for author ${SCHOLAR_ID}"

            HISTOGRAM_FNAME="${CACHE_DIR}"/"${SCHOLAR_ID}"-histogram.html.gz
            HISTOGRAM_URL="https://scholar.google.com/citations?view_op=citations_histogram&hl=en&user=${SCHOLAR_ID}"

            get "${HISTOGRAM_URL}" --load-cookies=cookies.txt --keep-session-cookies --save-cookies=cookies.txt  -O "${HISTOGRAM_FNAME}" 2>&1 | tee output.txt

            message 'Status codes:'
            grep '^ *HTTP/' output.txt | awk '{ print $2 }'

            grep '^ *HTTP/' output.txt | awk '{ print $2 }' | while read status_code
            do
                if [ ${status_code} -ge 500 ]
                then
                    notify_blocked; mysleep 600; continue;
                elif [ ${status_code} -eq 404 ]
                then
                    { message Got blocked by 404 page; notify_blocked; FAIL_404=histogram; break; }
                fi
            done

            # We can be blocked by a page that asks us to log in.
            got_blocked_by_login "${HISTOGRAM_FNAME}" && { message Got blocked by login screen; notify_blocked; mysleep 600; continue; }
            # Or by a 404 page:
            got_blocked_by_404 "${HISTOGRAM_FNAME}" && { message Got blocked by 404 page; notify_blocked; FAIL_404=histogram; break; }



            message Done with histogram, will now wait

            mysleep $((2 * ${QUERY_INTERVAL} ))

            GOT_HISTOGRAM=1
        fi

        if [ "${GOT_COLLEAGUES}" -eq 0 ]
        then
            # Crawl coauthors page.
            message "Getting coauthors page for author ${SCHOLAR_ID}"

            COLLEAGUES_FNAME="${CACHE_DIR}"/"${SCHOLAR_ID}"-colleagues.html.gz
            COLLEAGUES_URL="https://scholar.google.com/citations?view_op=list_colleagues&hl=en&user=${SCHOLAR_ID}"

            get "${COLLEAGUES_URL}" --load-cookies=cookies.txt --keep-session-cookies --save-cookies=cookies.txt  -O "${COLLEAGUES_FNAME}" 2>&1  | tee output.txt

            message 'Status codes:'
            grep '^ *HTTP/' output.txt | awk '{ print $2 }'

            grep '^ *HTTP/' output.txt | awk '{ print $2 }' | while read status_code
            do
                if [ ${status_code} -ge 500 ]
                then
                    notify_blocked; mysleep 600; continue;
                elif [ ${status_code} -eq 404 ]
                then
                    { message Got blocked by 404 page; notify_blocked; FAIL_404=histogram; break; }
                fi
            done

            # We can be blocked by a page that asks us to log in.
            got_blocked_by_login "${COLLEAGUES_FNAME}" && { message Got blocked by login screen; notify_blocked; mysleep 600; continue; }
            # Or by a 404 page:
            got_blocked_by_404 "${COLLEAGUES_FNAME}" && { message Got blocked by 404 page; notify_blocked; FAIL_404=colleagues; break; }


            message Done with colleagues, will now wait

            mysleep $((2 * ${QUERY_INTERVAL} ))

            GOT_COLLEAGUES=1
        fi

        AUTHOR_FNAME="${CACHE_DIR}"/"${SCHOLAR_ID}"-"${START}".html.gz

        COUNT=100
        # Crawl list of publications for author
        URL="https://scholar.google.com/citations?view_op=list_works&pagesize=${COUNT}&cstart=${START}&sortby=pubdate&hl=en&user=${REAL_SCHOLAR_ID}"

        get "${URL}" --load-cookies=cookies.txt --keep-session-cookies --save-cookies=cookies.txt  -O "${AUTHOR_FNAME}" || { notify_blocked; mysleep 600; continue; }

        # We can be blocked by a page that asks us to log in.
        got_blocked_by_login "${AUTHOR_FNAME}" && { message Got blocked by login screen; notify_blocked; mysleep 600; continue; }
        # Or by a 404 page:
        got_blocked_by_404 "${AUTHOR_FNAME}" && { message Got blocked by 404 page; notify_blocked; FAIL_404=list_works; break; }


        ANS=$(python author_go_on.py "${CACHE_DIR}" "${SCHOLAR_ID}"-"${START}")
        if [ "${ANS}" = "DONE" ]
        then
            message Done with author "${SCHOLAR_ID}"
            break
        else
            ((START+=COUNT))
            message Will continue for author "${SCHOLAR_ID}" starting from "${START}"

            mysleep "${QUERY_INTERVAL}"
            continue
        fi
    done

    if [ "${FAIL_404}" ]
    then
        # Got a 404 on some request.
        notify_crawling_failure_author "${SCHOLAR_ID}" "${FAIL_404}" 404
        return 0
    else
        # Crawling successful.
        notify_crawled_author "${SCHOLAR_ID}"
        python parse_author_and_add_to_db.py "${SCHOLAR_ID}"
        notify_added_author_to_db "${SCHOLAR_ID}"
        return 0
    fi
}

source config.sh

notify_started

notify_got_ip `wget 'https://api.ipify.org' -O - 2>/dev/null`

mkdir -p publications_cache
mkdir -p authors_cache

RENEW_QUEUE=1
REQUESTED_SCHOLAR_ID=
SPECIAL_REQUEST=0

while true
do
    case "${CRAWLING_MODE}" in
        PUBLICATION)
            QUEUE_FILE="publication-ids.txt"
            CRAWL_FUNCTION=crawl_publication
            if [ "${RENEW_QUEUE}" -eq 1 ]
            then
                message "Getting stale publications from SQL server..."
                python get_stale_publications.py 1000 > "${QUEUE_FILE}"
                notify_got_stale_publications `wc -l "${QUEUE_FILE}"`
            else
                notify_got_requested_publication "${REQUESTED_SCHOLAR_ID}"
                echo "${REQUESTED_SCHOLAR_ID}" > "${QUEUE_FILE}"
            fi

            if [ ! -s "${QUEUE_FILE}" ]
            then
                message 'No more stale publications. Done!'

                # Instead of exiting, we could maybe sleep a bit, and poll
                # for a command. Then `continue' instead of `break'.
                break
            fi
            ;;
        AUTHOR)
            QUEUE_FILE="author-ids.txt"
            CRAWL_FUNCTION=crawl_author

            if [ "${RENEW_QUEUE}" -eq 1 ]
            then
                message "Getting stale authors from SQL server..."
                python get_stale_authors.py 1000 > "${QUEUE_FILE}"
                notify_got_stale_authors `wc -l "${QUEUE_FILE}"`
            else
                notify_got_requested_author "${REQUESTED_SCHOLAR_ID}"
                echo "${REQUESTED_SCHOLAR_ID}" > "${QUEUE_FILE}"
            fi

            if [ ! -s "${QUEUE_FILE}" ]
            then
                message 'No more stale authors. Done!'

                # Instead of exiting, we could maybe sleep a bit, and poll
                # for a command. Then `continue' instead of `break'.
                break
            fi
            ;;
    esac

    rm -f cookies.txt

    notify_crawling_started

    MODE_CHANGE=0
    while read SCHOLAR_ID
    do
        message Will now "${CRAWL_FUNCTION}" "${SCHOLAR_ID}"
        "${CRAWL_FUNCTION}" "${SCHOLAR_ID}"

        poll_command
        message Supervisor says: "${COMMAND}" "${ARGUMENT1}" "${ARGUMENT2}"
        case "$COMMAND" in
            NONE)
                # No command, continue as before.
                message No command from supervisor
                ;;
            SET_MODE)
                # Got told to change mode to "$ARGUMENT1".

                CRAWLING_MODE="$ARGUMENT1"
                RENEW_QUEUE=1
                REQUESTED_SCHOLAR_ID=
                SPECIAL_REQUEST=0
                MODE_CHANGE=1
                message Will start crawling mode "${CRAWLING_MODE}".

                mysleep "${QUERY_INTERVAL}"
                break
                ;;
            CRAWL_SPECIFIC)
                # Got told to crawl a specific "$ARGUMENT1", with
                # scholar_id of "$ARGUMENT2".

                NORMAL_CRAWLING_MODE="${CRAWLING_MODE}"
                CRAWLING_MODE="$ARGUMENT1"
                RENEW_QUEUE=0
                REQUESTED_SCHOLAR_ID="$ARGUMENT2"
                SPECIAL_REQUEST=1
                MODE_CHANGE=1
                message Will crawl "${CRAWLING_MODE}" "${REQUESTED_SCHOLAR_ID}".

                mysleep "${QUERY_INTERVAL}"
                break
                ;;
        esac

        mysleep "${QUERY_INTERVAL}"
    done < "${QUEUE_FILE}"

    if [ "${SPECIAL_REQUEST}" -eq 1 -a "${MODE_CHANGE}" -eq 0 ]
    then
        message Done with special request for "${CRAWLING_MODE}" "${REQUESTED_SCHOLAR_ID}"
        # Done with the special request.
        SPECIAL_REQUEST=0
        REQUESTED_SCHOLAR_ID=
        CRAWLING_MODE="${NORMAL_CRAWLING_MODE}"
        RENEW_QUEUE=1
        message Reverting to mode "${CRAWLING_MODE}"
    fi

    if [ "${RENEW_QUEUE}" -eq 1 ]
    then
        RENEW_QUEUE=1
        mv "${QUEUE_FILE}" old-"${QUEUE_FILE}"
    fi
done

notify_exited
redtext Exiting
