from datetime import datetime, timedelta, timezone
from pathlib import Path

from serieux import Serieux
from serieux.features.fromfile import IncludeFile

from sarc.models.user import MemberType, User

from .common import Credential, Data, DataFactory, Supervision, Valid

ADJECTIVES = [
    "brave",
    "bright",
    "calm",
    "clever",
    "eager",
    "fancy",
    "gentle",
    "happy",
    "humble",
    "jolly",
    "keen",
    "lively",
    "merry",
    "noble",
    "proud",
    "quiet",
    "rapid",
    "sharp",
    "swift",
    "wise",
]

JOBS = [
    "archer",
    "baker",
    "clerk",
    "dancer",
    "farmer",
    "fisher",
    "gardener",
    "hunter",
    "joiner",
    "keeper",
    "logger",
    "miner",
    "nurse",
    "painter",
    "ranger",
    "sailor",
    "tailor",
    "vet",
    "weaver",
    "writer",
]

# Transition probabilities: from state -> [(next_state, weight), ...]
# Transitions only go "forward" (no phd -> master, etc.)
TRANSITION_MATRIX: dict[MemberType | None, list[tuple[MemberType | None, float]]] = {
    None: [
        (None, 80),
        (MemberType.MASTER_RESEARCH, 5),
        (MemberType.MASTER_PRO, 1),
        (MemberType.PHD_STUDENT, 3),
        (MemberType.INTERN, 2),
        (MemberType.STAFF, 3),
        (MemberType.PROFESSOR, 1),
    ],
    MemberType.INTERN: [
        (MemberType.INTERN, 4),
        (MemberType.MASTER_RESEARCH, 4),
        (MemberType.MASTER_PRO, 1),
        (None, 2),
    ],
    MemberType.MASTER_RESEARCH: [
        (MemberType.MASTER_RESEARCH, 80),
        (MemberType.PHD_STUDENT, 18),
        (None, 2),
    ],
    MemberType.PHD_STUDENT: [
        (MemberType.PHD_STUDENT, 90),
        (MemberType.POSTDOC, 10),
        (None, 5),
    ],
    MemberType.POSTDOC: [
        (MemberType.POSTDOC, 60),
        (MemberType.PROFESSOR, 30),
        (MemberType.STAFF, 5),
        (None, 15),
    ],
    MemberType.STAFF: [(MemberType.STAFF, 98), (None, 2)],
    # Professors only leave if they have no students (enforced in the simulation loop)
    MemberType.PROFESSOR: [(MemberType.PROFESSOR, 99), (None, 1)],
}

STUDENT_TYPES = {
    MemberType.MASTER_RESEARCH,
    MemberType.MASTER_PRO,
    MemberType.PHD_STUDENT,
}

# Probability of picking 1, 2, or 3 supervisors
SUPERVISOR_COUNT_WEIGHTS = [0.80, 0.15, 0.05]


def generate_users(self: DataFactory, data: Data):
    rng_names = self.get_rng("users:names")
    rng_sim = self.get_rng("users:simulation")
    rng_sup = self.get_rng("users:supervisors")

    pairs = [(adj, job) for adj in ADJECTIVES for job in JOBS]
    rng_names.shuffle(pairs)
    if len(pairs) < self.users.total:
        raise ValueError(
            f"Not enough name combinations ({len(pairs)}) for {self.users.total} users"
        )

    n_profs = self.users.full_professors
    n_total = self.users.total

    t_start_dt = datetime(
        self.t_start.year, self.t_start.month, self.t_start.day, tzinfo=timezone.utc
    )
    t_end_dt = datetime(
        self.t_end.year, self.t_end.month, self.t_end.day, tzinfo=timezone.utc
    )

    for i, (adj, job) in enumerate(pairs[:n_total]):
        user_id = i + 1
        display_name = f"{adj.capitalize()} {job.capitalize()}"
        email = f"{adj}.{job}@mila.quebec"
        data.users.append(User(id=user_id, display_name=display_name, email=email))

    # Current membership state per user
    states: dict[int, MemberType | None] = {}
    # Start of the current open interval per user (None if state is None)
    open_starts: dict[int, datetime | None] = {}
    # Current supervisor list per student user_id
    student_supervisors: dict[int, list[int]] = {}
    # Start of the current supervision interval per student user_id
    supervision_starts: dict[int, datetime] = {}

    # first_active: tick dt when user first gained any membership
    first_active: dict[int, datetime] = {}
    # gone_time: tick dt when user permanently left (transitioned to None from active state)
    gone_time: dict[int, datetime] = {}

    for uid in range(1, n_profs + 1):
        states[uid] = MemberType.PROFESSOR
        open_starts[uid] = t_start_dt
        first_active[uid] = t_start_dt
    for uid in range(n_profs + 1, n_total + 1):
        states[uid] = None
        open_starts[uid] = None

    def close_membership(uid: int, end_dt: datetime) -> None:
        if states[uid] is not None:
            data.memberships.append(
                Valid(
                    user_id=uid,
                    relationship=states[uid],
                    start=open_starts[uid],
                    end=end_dt,
                )
            )

    def close_supervision(uid: int, end_dt: datetime) -> None:
        if uid in student_supervisors:
            data.supervisions.append(
                Valid(
                    user_id=uid,
                    relationship=Supervision(
                        supervisor_ids=student_supervisors.pop(uid)
                    ),
                    start=supervision_starts.pop(uid),
                    end=end_dt,
                )
            )

    t = self.t_start
    while t < self.t_end:
        dt = datetime(t.year, t.month, t.day, tzinfo=timezone.utc)

        # Snapshot professors at the start of this tick (used for supervisor assignment)
        current_profs = [uid for uid, s in states.items() if s == MemberType.PROFESSOR]

        # Step 1: transition all non-professor users (skip those permanently gone)
        for uid in range(1, n_total + 1):
            if states[uid] == MemberType.PROFESSOR or uid in gone_time:
                continue
            current = states[uid]
            next_states_list, weights = zip(*TRANSITION_MATRIX[current])
            (next_state,) = rng_sim.choices(next_states_list, weights=weights)

            if next_state != current:
                close_membership(uid, dt)
                if current in STUDENT_TYPES:
                    close_supervision(uid, dt)
                states[uid] = next_state
                open_starts[uid] = dt if next_state is not None else None
                if next_state is None and current is not None:
                    gone_time[uid] = dt
                elif next_state is not None and uid not in first_active:
                    first_active[uid] = dt
                if next_state in STUDENT_TYPES and current_profs:
                    n_sups = rng_sup.choices(
                        [1, 2, 3], weights=SUPERVISOR_COUNT_WEIGHTS
                    )[0]
                    chosen = rng_sup.sample(
                        current_profs, min(n_sups, len(current_profs))
                    )
                    student_supervisors[uid] = chosen
                    supervision_starts[uid] = dt

        # Step 2: professors with no current students may leave per TRANSITION_MATRIX
        for uid in current_profs:
            has_students = any(uid in sups for sups in student_supervisors.values())
            if has_students:
                continue
            next_states_list, weights = zip(*TRANSITION_MATRIX[MemberType.PROFESSOR])
            (next_state,) = rng_sim.choices(next_states_list, weights=weights)
            if next_state != MemberType.PROFESSOR:
                close_membership(uid, dt)
                states[uid] = next_state
                open_starts[uid] = dt if next_state is not None else None
                gone_time[uid] = dt

        t += self.tick

    # Close all open intervals at t_end
    for uid in range(1, n_total + 1):
        close_membership(uid, t_end_dt)
        close_supervision(uid, t_end_dt)

    # Credentials: one entry per (user, domain) for users who were ever active.
    # Username format depends on domain: flast for mila, firlas for drac, first.last otherwise.
    # Domain probabilities: mila 0.9, drac 0.65, other 0.5.
    rng_cred = self.get_rng("credentials")
    domain_prob = {"mila": 0.9, "drac": 0.65}
    domains = {cfg.user_domain for cfg in self.clusters.values()}

    def make_username(adj: str, job: str, domain: str) -> str:
        if domain == "mila":
            return f"{adj[0]}{job}"
        elif domain == "drac":
            return f"{adj[:3]}{job[:3]}"
        else:
            return f"{adj}.{job}"

    for uid in range(1, n_total + 1):
        if uid not in first_active:
            continue
        adj, job = pairs[uid - 1]
        cred_start = first_active[uid]
        cred_end = gone_time.get(uid, t_end_dt)
        for domain in sorted(domains):
            if rng_cred.random() < domain_prob.get(domain, 0.5):
                data.credentials.append(
                    Valid(
                        user_id=uid,
                        relationship=Credential(
                            domain=domain, username=make_username(adj, job, domain)
                        ),
                        start=cred_start,
                        end=cred_end,
                    )
                )


MEMBER_STYLE: dict[MemberType | None, tuple[str, str]] = {
    None: ("\033[90m", "."),  # dark gray
    MemberType.INTERN: ("\033[97m", "I"),  # white
    MemberType.MASTER_RESEARCH: ("\033[94m", "M"),  # blue
    MemberType.MASTER_PRO: ("\033[94m", "M"),  # blue
    MemberType.PHD_STUDENT: ("\033[96m", "P"),  # cyan
    MemberType.POSTDOC: ("\033[92m", "D"),  # green
    MemberType.PROFESSOR: ("\033[93m", "F"),  # yellow
    MemberType.STAFF: ("\033[95m", "S"),  # magenta
}
RESET = "\033[0m"
N_COLS = 60


def user_table(data: Data):
    starts = [v.start for v in data.memberships if v.start is not None]
    ends = [v.end for v in data.memberships if v.end is not None]
    if not starts:
        return
    t_min = min(starts)
    t_max = max(ends) if ends else max(starts)
    span = (t_max - t_min).total_seconds()

    sample_times = [t_min + timedelta(seconds=span * i / N_COLS) for i in range(N_COLS)]

    by_user: dict[int, list[Valid[MemberType]]] = {}
    for v in data.memberships:
        by_user.setdefault(v.user_id, []).append(v)

    def state_at(intervals: list[Valid[MemberType]], t: datetime) -> MemberType | None:
        for v in intervals:
            if (v.start is None or v.start <= t) and (v.end is None or t < v.end):
                return v.relationship
        return None

    # Legend
    legend = "  ".join(
        f"{color}{letter}{RESET}={mtype.value if mtype else 'none'}"
        for mtype, (color, letter) in MEMBER_STYLE.items()
    )
    print(f"{'':20}  {legend}\n")

    for user in data.users:
        intervals = by_user.get(user.id, [])
        cells = "".join(
            f"{color}{letter}{RESET}"
            for t in sample_times
            for color, letter in [MEMBER_STYLE[state_at(intervals, t)]]
        )
        print(f"{user.display_name:<20}  {cells}")


if __name__ == "__main__":
    srx = (Serieux + IncludeFile)()
    factory = srx.deserialize(
        DataFactory, Path(__file__).parent / "factory-config.yaml"
    )
    data = Data()
    generate_users(factory, data)
    user_table(data)
