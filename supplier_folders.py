import os
import paramiko

supplier_remote_folders = [
    [
        ""
    ],
    [
        ""
    ],
    [
        ""
    ],
    [
        ""
    ],
    [
        "/mnt/feeds_data/i-0bd644c5c4eb7964c/metric_connector/METRIC",
        "/mnt/feeds_data/i-05dbabedb7c00a400/metric_connector/METRIC",
        "/mnt/feeds_data/i-02d43a54f70f039dc/metric_connector/METRIC",
        "/mnt/feeds_data/i-08a740323b88a36c9/metric_connector/METRIC",
        "/mnt/feeds_data/i-087c03bd2ea8928f9/metric_connector/METRIC",
        "/mnt/feeds_data/i-0f18f3cc11c8d2640/metric_connector/METRIC"
    ],
    [
        "/mnt/feeds_data/i-03230d6944a3b8bdc/feed_normalizer/PA",
        "/mnt/feeds_data/i-0f42926fb921581ff/feed_normalizer/PA"
    ],
    [
        "/mnt/feeds_data/i-0570b41c2388f6bf6/feed_normalizer/PAGH",
        "/mnt/feeds_data/i-0e66b2dab246a6408/feed_normalizer/PAGH"
    ],
    [
        "/mnt/feeds_data/i-013525a6f4ea171e5/feed_normalizer/SSOL",
        "/mnt/feeds_data/i-04a897aef38983212/feed_normalizer/SSOL",
        "/mnt/feeds_data/i-0e772b33e02f9c83e/feed_normalizer/SSOL"
    ],
    [
        "/mnt/feeds_data/i-013525a6f4ea171e5/drivein/SSOLIN",
        "/mnt/feeds_data/i-04a897aef38983212/drivein/SSOLIN",
        "/mnt/feeds_data/i-0e772b33e02f9c83e/drivein/SSOLIN"
    ],
    [
        "/mnt/feeds_data/i-05dbabedb7c00a400/feed_connector/BGIN",
        "/mnt/feeds_data/i-0bd644c5c4eb7964c/feed_connector/BGIN",
        "/mnt/feeds_data/i-02d43a54f70f039dc/feed_connector/BGIN",
        "/mnt/feeds_data/i-08a740323b88a36c9/feed_connector/BGIN",
        "/mnt/feeds_data/i-087c03bd2ea8928f9/feed_connector/BGIN",
        "/mnt/feeds_data/i-0f18f3cc11c8d2640/feed_connector/BGIN"
    ]
]

def choose_supplier_directories(supplier, hostname, username, password):
    valid_folders = []
    ssh_client=paramiko.SSHClient()
    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
    ssh_client.connect(hostname=hostname, username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
    ftp_client=ssh_client.open_sftp()
    for i in supplier_remote_folders[supplier]:
        try:
            ftp_client.stat(i)
            valid_folders.append(i)
        except FileNotFoundError:
            continue
    return valid_folders