# squid_energy.py

import pandas as pd

def calculate_squid_energy(group):
    # `group` is a DataFrame of hatches for a single `address`

    # Sort `group` by `created_at`
    group = group.sort_values('created_at')

    # Initialize `squid_energy` and `time_checkpoint`
    squid_energy = 0
    time_checkpoint = None

    # For each hatch in `group`:
    for index, row in group.iterrows():
        amt = row['amt']
        created_at = row['created_at']

        # If `time_checkpoint` is not None, calculate time difference `delta_t` between current `created_at` and `time_checkpoint`
        if time_checkpoint is not None:
            # Calculate time difference `delta_t` in seconds
            delta_t = (created_at - time_checkpoint).total_seconds()

            # Calculate the number of half-lives that have passed
            half_lives = delta_t / (14 * 24 * 60 * 60)

            print(f'delta_t: {delta_t} seconds, half_lives: {half_lives}')

            # Calculate decayed squid energy `decayed_energy` = `squid_energy` * 0.5 ^ `half_lives`
            squid_energy = squid_energy * (0.5 ** half_lives)

        # Update `squid_energy` to `decayed_energy` + `amt`
        squid_energy = squid_energy + amt

        # Update `time_checkpoint` to current `created_at`
        time_checkpoint = created_at

    # Return a new entry with `address`, `squid_energy`, and `time_checkpoint`
    return {'address': group['address'].iloc[0], 'squid_energy': squid_energy, 'time_checkpoint': time_checkpoint}