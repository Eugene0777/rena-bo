document.addEventListener('DOMContentLoaded', () => {
    let userData = [];
    const discordBody = document.getElementById('discord-body');
    const twitterBody = document.getElementById('twitter-body');
    const discordSearch = document.getElementById('discord-search');
    const twitterSearch = document.getElementById('twitter-search');
    const discordSort = document.getElementById('discord-sort');
    const discordFilter = document.getElementById('discord-filter');
    const twitterSort = document.getElementById('twitter-sort');
    const tabBtns = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    // Tab Switching
    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            tabBtns.forEach(b => b.classList.remove('active'));
            tabContents.forEach(c => c.classList.remove('active'));

            btn.classList.add('active');
            const tabId = btn.getAttribute('data-tab');
            document.getElementById(`${tabId}-view`).classList.add('active');
        });
    });

    // Fetch Data
    fetch('user_stats.json')
        .then(response => response.json())
        .then(data => {
            userData = data;
            populateChannels(data);
            renderDiscord();
            renderTwitter();
        })
        .catch(err => {
            console.error('Error loading data:', err);
            discordBody.innerHTML = '<div class="loading">Error loading data. Make sure to run via local server.</div>';
            twitterBody.innerHTML = '<div class="loading">Error loading data.</div>';
        });

    function populateChannels(data) {
        const channels = new Set();
        data.forEach(user => {
            if (user.message_count) {
                Object.keys(user.message_count).forEach(ch => {
                    // Exclude choose-region and other utility channels
                    if (ch.toLowerCase().includes('choose-region')) return;

                    // Include any channel that has '｜' (language indicator) or 'general'
                    if (ch.includes('｜') || ch.toLowerCase().includes('general')) {
                        channels.add(ch);
                    }
                });
            }
        });

        const sortedChannels = Array.from(channels).sort((a, b) => {
            if (a.toLowerCase().includes('general')) return -1;
            if (b.toLowerCase().includes('general')) return 1;
            return a.localeCompare(b);
        });

        discordFilter.innerHTML = '';

        // Add "ALL" as the first option
        const allOpt = document.createElement('option');
        allOpt.value = 'all';
        allOpt.textContent = 'ALL CHANNELS';
        discordFilter.appendChild(allOpt);

        sortedChannels.forEach(ch => {
            const opt = document.createElement('option');
            opt.value = ch;
            let displayName = ch.includes('｜') ? ch.split('｜').pop() : ch;
            displayName = displayName.includes('⎮') ? displayName.split('⎮').pop() : displayName;
            opt.textContent = displayName.trim().toUpperCase();
            discordFilter.appendChild(opt);
        });

        discordFilter.value = 'all';
    }

    function getDiscordStats(user, channel = 'all') {
        let count = 0;
        let favChannel = 'N/A';
        let maxMsgs = -1;

        if (user.message_count) {
            if (channel === 'all') {
                for (const [ch, c] of Object.entries(user.message_count)) {
                    count += c;
                    if (c > maxMsgs) {
                        maxMsgs = c;
                        favChannel = ch;
                    }
                }
            } else {
                count = user.message_count[channel] || 0;
                for (const [ch, c] of Object.entries(user.message_count)) {
                    if (c > maxMsgs) {
                        maxMsgs = c;
                        favChannel = ch;
                    }
                }
            }
        }
        return { total: count, favChannel };
    }

    function renderDiscord() {
        const searchTerm = discordSearch.value.toLowerCase();
        const sortBy = discordSort.value;
        const channelFilter = discordFilter.value;

        let filtered = userData.map(user => {
            const stats = getDiscordStats(user, channelFilter);
            return { ...user, ...stats };
        }).filter(user => {
            const name = user.user_name.toLowerCase();
            return name.includes(searchTerm) && name !== 'mee6' && name !== 'rena bot' && user.total > 0;
        });

        if (sortBy === 'messages') {
            filtered.sort((a, b) => b.total - a.total);
        } else if (sortBy === 'name') {
            filtered.sort((a, b) => a.user_name.localeCompare(b.user_name));
        }

        // Limit to top 100 for performance
        const displayList = filtered.slice(0, 100);

        discordBody.innerHTML = '';
        displayList.forEach((user, index) => {
            const row = document.createElement('div');
            row.className = `tr rank-${index + 1}`;
            row.innerHTML = `
                <div class="td rank">${index + 1}</div>
                <div class="td user">
                    <div class="user-cell">
                        <img src="${user.pfp}" alt="${user.user_name}" class="pfp" onerror="this.src='https://cdn.discordapp.com/embed/avatars/0.png'">
                        <span class="username">${user.user_name}</span>
                    </div>
                </div>
                <div class="td messages">${user.total.toLocaleString()}</div>
                <div class="td fav-channel">${user.favChannel}</div>
            `;
            discordBody.appendChild(row);
        });
    }

    function renderTwitter() {
        const searchTerm = twitterSearch.value.toLowerCase();
        const sortBy = twitterSort.value;

        let filtered = userData
            .filter(user => user.twitter_name && user.twitter_stats)
            .filter(user => user.user_name.toLowerCase().includes(searchTerm) || user.twitter_name.toLowerCase().includes(searchTerm));

        if (sortBy === 'views') {
            filtered.sort((a, b) => (b.twitter_stats.посмотры || 0) - (a.twitter_stats.посмотры || 0));
        } else if (sortBy === 'likes') {
            filtered.sort((a, b) => (b.twitter_stats.like || 0) - (a.twitter_stats.like || 0));
        } else if (sortBy === 'posts') {
            filtered.sort((a, b) => (b.twitter_stats.post || 0) - (a.twitter_stats.post || 0));
        }

        // Limit to top 100 for performance
        const displayList = filtered.slice(0, 100);

        twitterBody.innerHTML = '';
        displayList.forEach((user, index) => {
            const stats = user.twitter_stats;
            const row = document.createElement('div');
            row.className = `tr rank-${index + 1}`;
            row.innerHTML = `
                <div class="td rank">${index + 1}</div>
                <div class="td user">
                    <div class="user-cell">
                        <img src="${user.pfp}" alt="${user.user_name}" class="pfp" onerror="this.src='https://cdn.discordapp.com/embed/avatars/0.png'">
                        <span class="username">${user.user_name}</span>
                    </div>
                </div>
                <div class="td twitter-handle">@${user.twitter_name}</div>
                <div class="td metric">${stats.post || 0}</div>
                <div class="td metric">${stats.like || 0}</div>
                <div class="td metric">${stats.reply || 0}</div>
                <div class="td metric">${stats.ретвит || 0}</div>
                <div class="td metric">${stats.цитата || 0}</div>
                <div class="td metric views">${(stats.посмотры || 0).toLocaleString()}</div>
            `;
            twitterBody.appendChild(row);
        });
    }

    // Event Listeners for Filters
    discordSearch.addEventListener('input', renderDiscord);
    discordSort.addEventListener('change', renderDiscord);
    discordFilter.addEventListener('change', renderDiscord);
    twitterSearch.addEventListener('input', renderTwitter);
    twitterSort.addEventListener('change', renderTwitter);
});
